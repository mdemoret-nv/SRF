/**
 * SPDX-FileCopyrightText: Copyright (c) 2021-2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <pysrf/pipeline.hpp>

#include <pysrf/types.hpp>

#include <srf/node/rx_node.hpp>
#include <srf/pipeline/pipeline.hpp>
#include <srf/segment/builder.hpp>

#include <pybind11/gil.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>  // for move
#include "pysrf/node.hpp"
#include "srf/segment/forward.hpp"

namespace {
namespace py = pybind11;

template <std::size_t Count, template <class...> class PortClass, typename... ArgsT>
struct Pipeline__PortBuilder : Pipeline__PortBuilder<Count - 1, PortClass, srf::pysrf::PyHolder, ArgsT...>
{};

template <template <class...> class PortClass, typename... ArgsT>
struct Pipeline__PortBuilder<0, PortClass, ArgsT...>
{
    using port_type_t                  = PortClass<ArgsT...>;
    static constexpr size_t port_count = sizeof...(ArgsT);

    static port_type_t build(const std::vector<std::string>& port_ids)
    {
        return port_type_t(port_ids);
    }
};
}  // namespace

namespace srf::pysrf {

namespace py = pybind11;

template <typename T>
class PythonIngressPort : public segment::Object<node::SourceProperties<T>>, public segment::IngressPortBase
{
    // tap for debugging
    // rxcpp::operators::tap([this](const T& t) {
    //     LOG(INFO) << segment::info(m_segment_address) << "ingress port " << m_port_name << ": tap = " << t;
    // })

  public:
    PythonIngressPort(SegmentAddress address, PortName name) :
      m_segment_address(address),
      m_port_name(std::move(name)),
      m_source(std::make_unique<PythonNode<T, T>>())
    {
        this->m_name = m_port_name;
    }

  private:
    node::SourceProperties<T>* get_object() const final
    {
        CHECK(m_source);
        return m_source.get();
    }

    std::unique_ptr<runnable::Launcher> prepare_launcher(runnable::LaunchControl& launch_control) final
    {
        std::lock_guard<decltype(m_mutex)> lock(m_mutex);
        CHECK(m_source);
        return launch_control.prepare_launcher(std::move(m_source));
    }

    std::shared_ptr<manifold::Interface> make_manifold(pipeline::Resources& resources) final
    {
        return manifold::Factory<T>::make_manifold(m_port_name, resources);
    }

    void connect_to_manifold(std::shared_ptr<manifold::Interface> manifold) final
    {
        // ingress ports connect to manifold outputs
        std::lock_guard<decltype(m_mutex)> lock(m_mutex);
        CHECK(m_source);
        manifold->add_output(m_segment_address, m_source.get());
    }

    SegmentAddress m_segment_address;
    PortName m_port_name;
    std::unique_ptr<PythonNode<T, T>> m_source;
    std::mutex m_mutex;

    friend segment::Instance;
};

template <typename... TypesT>
struct PythonIngressPorts : public segment::IngressPortsBase
{
    using port_builder_fn_t = typename IngressPortsBase::port_builder_fn_t;

    PythonIngressPorts(std::vector<std::string> names) : IngressPortsBase(std::move(names), get_builders()) {}

  private:
    static std::vector<port_builder_fn_t> get_builders()
    {
        std::vector<port_builder_fn_t> builders;
        (builders.push_back([](const SegmentAddress& address, const PortName& name) {
            return std::make_shared<PythonIngressPort<TypesT>>(address, name);
        }),
         ...);

        return builders;
    }
};

template <typename T>
class PythonEgressPort final : public segment::Object<node::SinkProperties<T>>,
                               public segment::EgressPortBase,
                               public std::enable_shared_from_this<PythonEgressPort<T>>
{
    // debug tap
    // rxcpp::operators::tap([this](const T& t) {
    //     LOG(INFO) << segment::info(m_segment_address) << " egress port " << m_port_name << ": tap = " << t;
    // })

  public:
    PythonEgressPort(SegmentAddress address, PortName name) :
      m_segment_address(address),
      m_port_name(std::move(name)),
      m_sink(std::make_unique<PythonNode<T, T>>())
    {
        this->m_name = m_port_name;
    }

  private:
    node::SinkProperties<T>* get_object() const final
    {
        CHECK(m_sink) << "failed to acquire backing runnable for egress port " << m_port_name;
        return m_sink.get();
    }

    std::unique_ptr<runnable::Launcher> prepare_launcher(runnable::LaunchControl& launch_control) final
    {
        std::lock_guard<decltype(m_mutex)> lock(m_mutex);
        CHECK(m_sink);
        CHECK(m_manifold_connected) << "manifold not set for egress port";
        return launch_control.prepare_launcher(std::move(m_sink));
    }

    std::shared_ptr<manifold::Interface> make_manifold(pipeline::Resources& resources) final
    {
        return manifold::Factory<T>::make_manifold(m_port_name, resources);
    }

    void connect_to_manifold(std::shared_ptr<manifold::Interface> manifold) final
    {
        // egress ports connect to manifold inputs
        std::lock_guard<decltype(m_mutex)> lock(m_mutex);
        DCHECK_EQ(manifold->port_name(), m_port_name);
        CHECK(m_sink);
        CHECK(!m_manifold_connected);
        manifold->add_input(m_segment_address, m_sink.get());
        m_manifold_connected = true;
    }

    SegmentAddress m_segment_address;
    PortName m_port_name;
    std::unique_ptr<PythonNode<T, T>> m_sink;
    bool m_manifold_connected{false};
    runnable::LaunchOptions m_launch_options;
    std::mutex m_mutex;
};

template <typename... TypesT>
struct PythonEgressPorts : public segment::EgressPortsBase
{
    using port_builder_fn_t = typename segment::EgressPortsBase::port_builder_fn_t;

    PythonEgressPorts(std::vector<std::string> names) : EgressPortsBase(std::move(names), get_builders()) {}

  private:
    static std::vector<port_builder_fn_t> get_builders()
    {
        std::vector<port_builder_fn_t> builders;
        (builders.push_back([](const SegmentAddress& address, const PortName& name) {
            return std::make_shared<PythonEgressPort<TypesT>>(address, name);
        }),
         ...);

        return builders;
    }
};

template <typename... TypesT>
using IngressPortsT = PythonIngressPorts<TypesT...>;

template <typename... TypesT>
using EgressPortsT = PythonEgressPorts<TypesT...>;

Pipeline::Pipeline() : m_pipeline(srf::pipeline::make_pipeline()) {}

void Pipeline::make_segment(const std::string& name, const std::function<void(srf::segment::Builder&)>& init)
{
    auto init_wrapper = [=](srf::segment::Builder& seg) {
        py::gil_scoped_acquire gil;
        init(seg);
    };

    m_pipeline->make_segment(name, init_wrapper);
}

void Pipeline::make_segment(const std::string& name,
                            const std::vector<std::string>& ingress_port_ids,
                            const std::vector<std::string>& egress_port_ids,
                            const std::function<void(srf::segment::Builder&)>& init)
{
    if (ingress_port_ids.empty() && egress_port_ids.empty())
    {
        return make_segment(name, init);
    }

    auto init_wrapper = [init](srf::segment::Builder& seg) {
        py::gil_scoped_acquire gil;
        init(seg);
    };

    if (!ingress_port_ids.empty() && !egress_port_ids.empty())
    {
        dynamic_port_config(name, ingress_port_ids, egress_port_ids, init_wrapper);
    }
    else if (!ingress_port_ids.empty())
    {
        dynamic_port_config_ingress(name, ingress_port_ids, init);
    }
    else
    {
        dynamic_port_config_egress(name, egress_port_ids, init);
    }
}

// Need to have all supported cases explicitly enumerated so python bindings work as expected.
void Pipeline::dynamic_port_config(const std::string& name,
                                   const std::vector<std::string>& ingress_port_ids,
                                   const std::vector<std::string>& egress_port_ids,
                                   const std::function<void(srf::segment::Builder&)>& init)
{
    if (ingress_port_ids.empty() || ingress_port_ids.size() > SRF_MAX_INGRESS_PORTS)
    {
        throw std::runtime_error("Maximum of 10 egress ports supported via python interface");
    }

    if (ingress_port_ids.size() == 1)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<1, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
    else if (ingress_port_ids.size() == 2)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<2, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
    else if (ingress_port_ids.size() == 3)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<3, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
    else if (ingress_port_ids.size() == 4)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<4, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
    else if (ingress_port_ids.size() == 5)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<5, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
    else if (ingress_port_ids.size() == 6)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<6, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
    else if (ingress_port_ids.size() == 7)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<7, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
    else if (ingress_port_ids.size() == 8)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<8, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
    else if (ingress_port_ids.size() == 9)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<9, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
    else if (ingress_port_ids.size() == 10)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<10, IngressPortsT>::build(ingress_port_ids);
        _dynamic_port_config_egress(name, ingress_ports, egress_port_ids, init);
    }
}

// Need to have all supported cases enumerated for python
void Pipeline::dynamic_port_config_ingress(const std::string& name,
                                           const std::vector<std::string>& ingress_port_ids,
                                           const std::function<void(srf::segment::Builder&)>& init)
{
    if (ingress_port_ids.empty() || ingress_port_ids.size() > SRF_MAX_INGRESS_PORTS)
    {
        throw std::runtime_error("Maximum of 10 egress ports supported via python interface");
    }

    if (ingress_port_ids.size() == 1)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<1, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else if (ingress_port_ids.size() == 2)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<2, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else if (ingress_port_ids.size() == 3)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<3, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else if (ingress_port_ids.size() == 4)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<4, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else if (ingress_port_ids.size() == 5)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<5, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else if (ingress_port_ids.size() == 6)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<6, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else if (ingress_port_ids.size() == 7)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<7, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else if (ingress_port_ids.size() == 8)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<8, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else if (ingress_port_ids.size() == 9)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<9, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else if (ingress_port_ids.size() == 10)
    {
        auto ingress_ports = ::Pipeline__PortBuilder<10, IngressPortsT>::build(ingress_port_ids);
        m_pipeline->make_segment(name, ingress_ports, init);
    }
    else
    {
        throw std::runtime_error("Maximum of 10 ingress ports supported via python interface");
    }
}

// Need to have all supported cases enumerated for python
void Pipeline::dynamic_port_config_egress(const std::string& name,
                                          const std::vector<std::string>& egress_port_ids,
                                          const std::function<void(srf::segment::Builder&)>& init)
{
    if (egress_port_ids.empty() || egress_port_ids.size() > SRF_MAX_EGRESS_PORTS)
    {
        throw std::runtime_error("Maximum of 10 egress ports supported via python interface");
    }

    if (egress_port_ids.size() == 1)
    {
        auto egress_ports = ::Pipeline__PortBuilder<1, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
    else if (egress_port_ids.size() == 2)
    {
        auto egress_ports = ::Pipeline__PortBuilder<2, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
    else if (egress_port_ids.size() == 3)
    {
        auto egress_ports = ::Pipeline__PortBuilder<3, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
    else if (egress_port_ids.size() == 4)
    {
        auto egress_ports = ::Pipeline__PortBuilder<4, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
    else if (egress_port_ids.size() == 5)
    {
        auto egress_ports = ::Pipeline__PortBuilder<5, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
    else if (egress_port_ids.size() == 6)
    {
        auto egress_ports = ::Pipeline__PortBuilder<6, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
    else if (egress_port_ids.size() == 7)
    {
        auto egress_ports = ::Pipeline__PortBuilder<7, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
    else if (egress_port_ids.size() == 8)
    {
        auto egress_ports = ::Pipeline__PortBuilder<8, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
    else if (egress_port_ids.size() == 9)
    {
        auto egress_ports = ::Pipeline__PortBuilder<9, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
    else if (egress_port_ids.size() == 10)
    {
        auto egress_ports = ::Pipeline__PortBuilder<10, EgressPortsT>::build(egress_port_ids);
        m_pipeline->make_segment(name, egress_ports, init);
    }
}

template <typename... IngressPortTypesT>
void Pipeline::_dynamic_port_config_egress(const std::string& name,
                                           const segment::IngressPortsBase& ingress_ports,
                                           const std::vector<std::string>& egress_port_ids,
                                           const std::function<void(srf::segment::Builder&)>& init)
{
    if (egress_port_ids.empty() || egress_port_ids.size() > SRF_MAX_EGRESS_PORTS)
    {
        throw std::runtime_error("Maximum of 10 egress ports supported via python interface");
    }

    if (egress_port_ids.size() == 1)
    {
        auto egress_ports = ::Pipeline__PortBuilder<1, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
    else if (egress_port_ids.size() == 2)
    {
        auto egress_ports = ::Pipeline__PortBuilder<2, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
    else if (egress_port_ids.size() == 3)
    {
        auto egress_ports = ::Pipeline__PortBuilder<3, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
    else if (egress_port_ids.size() == 4)
    {
        auto egress_ports = ::Pipeline__PortBuilder<4, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
    else if (egress_port_ids.size() == 5)
    {
        auto egress_ports = ::Pipeline__PortBuilder<5, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
    else if (egress_port_ids.size() == 6)
    {
        auto egress_ports = ::Pipeline__PortBuilder<6, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
    else if (egress_port_ids.size() == 7)
    {
        auto egress_ports = ::Pipeline__PortBuilder<7, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
    else if (egress_port_ids.size() == 8)
    {
        auto egress_ports = ::Pipeline__PortBuilder<8, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
    else if (egress_port_ids.size() == 9)
    {
        auto egress_ports = ::Pipeline__PortBuilder<9, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
    else if (egress_port_ids.size() == 10)
    {
        auto egress_ports = ::Pipeline__PortBuilder<10, EgressPortsT>::build(egress_port_ids);
        m_pipeline->template make_segment(name, ingress_ports, egress_ports, init);
    }
}

std::unique_ptr<srf::pipeline::Pipeline> Pipeline::swap()
{
    auto tmp   = std::move(m_pipeline);
    m_pipeline = srf::pipeline::make_pipeline();
    return std::move(tmp);
}
}  // namespace srf::pysrf
