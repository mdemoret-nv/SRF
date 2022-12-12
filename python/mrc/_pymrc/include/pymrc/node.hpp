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

#pragma once

#include "pymrc/edge_adapter.hpp"
#include "pymrc/port_builders.hpp"
#include "pymrc/types.hpp"
#include "pymrc/utils.hpp"

#include "mrc/channel/ingress.hpp"
#include "mrc/channel/status.hpp"
#include "mrc/node/edge.hpp"
#include "mrc/node/edge_connector.hpp"
#include "mrc/node/forward.hpp"  // IWYU pragma: keep
#include "mrc/node/rx_node.hpp"
#include "mrc/node/rx_sink.hpp"
#include "mrc/node/rx_source.hpp"
#include "mrc/runnable/context.hpp"

#include <pybind11/cast.h>
#include <pybind11/gil.h>
#include <pybind11/pytypes.h>
#include <rxcpp/rx.hpp>

#include <functional>
#include <memory>
#include <utility>

// Avoid forward declaring template specialization base classes
// IWYU pragma: no_forward_declare mrc::node::Edge

namespace mrc {

namespace node {

template <typename SourceT>
class ConvertingEdgeWritable<SourceT,
                             pymrc::PyHolder,
                             std::enable_if_t<!pybind11::detail::is_pyobject<SourceT>::value &&
                                                  !std::is_convertible_v<SourceT, pybind11::object>,
                                              void>> : public ConvertingEdgeWritableBase<SourceT, pymrc::PyHolder>
{
    using base_t = ConvertingEdgeWritableBase<SourceT, pymrc::PyHolder>;
    using typename base_t::sink_t;
    using typename base_t::source_t;

    using base_t::ConvertingEdgeWritableBase;

    // We need to hold the GIL here, because casting from c++ -> pybind11::object allocates memory with Py_Malloc.
    // Its also important to note that you do not want to hold the GIL when calling m_output->await_write, as
    // that can trigger a deadlock with another fiber reading from the end of the channel

    channel::Status await_write(source_t&& data) final
    {
        pymrc::PyHolder py_data;
        {
            pybind11::gil_scoped_acquire gil;
            py_data = pybind11::cast(std::move(data));
        }

        return this->downstream().await_write(std::move(py_data));
    };
};

template <typename SinkT>
struct ConvertingEdgeWritable<
    pymrc::PyHolder,
    SinkT,
    std::enable_if_t<!pybind11::detail::is_pyobject<SinkT>::value && !std::is_convertible_v<pybind11::object, SinkT>,
                     void>> : public ConvertingEdgeWritableBase<pymrc::PyHolder, SinkT>
{
    using base_t = ConvertingEdgeWritableBase<pymrc::PyHolder, SinkT>;
    using typename base_t::sink_t;
    using typename base_t::source_t;

    using base_t::ConvertingEdgeWritableBase;

    // We don't hold the GIL in any of the *_write because we are explciitly releasing object's pointer, and casting it
    // to a c++ data type.

    channel::Status await_write(source_t&& data) override
    {
        sink_t _data;
        {
            pybind11::gil_scoped_acquire gil;
            _data = pybind11::cast<sink_t>(pybind11::object(std::move(data)));
        }

        return this->downstream().await_write(std::move(_data));
    }

    static void register_converter()
    {
        EdgeConnector<source_t, sink_t>::register_converter();
    }
};

template <>
struct ConvertingEdgeWritable<pymrc::PyObjectHolder, pybind11::object, void>
  : public ConvertingEdgeWritableBase<pymrc::PyObjectHolder, pybind11::object>
{
    using base_t = ConvertingEdgeWritableBase<pymrc::PyObjectHolder, pybind11::object>;
    using typename base_t::sink_t;
    using typename base_t::source_t;

    using base_t::ConvertingEdgeWritableBase;

    // We need to hold the GIL here, because casting from c++ -> pybind11::object allocates memory with Py_Malloc.
    // Its also important to note that you do not want to hold the GIL when calling m_output->await_write, as
    // that can trigger a deadlock with another fiber reading from the end of the channel

    channel::Status await_write(source_t&& data) final
    {
        pymrc::AcquireGIL gil;

        pybind11::object py_data = std::move(data);

        gil.release();

        return this->downstream().await_write(std::move(py_data));
    };
};

template <>
struct ConvertingEdgeWritable<pybind11::object, pymrc::PyObjectHolder, void>
  : public ConvertingEdgeWritableBase<pybind11::object, pymrc::PyObjectHolder>
{
    using base_t = ConvertingEdgeWritableBase<pybind11::object, pymrc::PyObjectHolder>;
    using typename base_t::sink_t;
    using typename base_t::source_t;

    using base_t::ConvertingEdgeWritableBase;

    // We don't hold the GIL in any of the *_write because we are explciitly releasing object's pointer, and casting it
    // to a c++ data type.

    channel::Status await_write(source_t&& data) override
    {
        // No need for the GIL
        sink_t _data = pymrc::PyObjectHolder(std::move(data));

        return this->downstream().await_write(std::move(_data));
    }

    static void register_converter()
    {
        EdgeConnector<source_t, sink_t>::register_converter();
    }
};

}  // namespace node

namespace pymrc {

// Export everything in the mrc::pymrc namespace by default since we compile with -fvisibility=hidden
#pragma GCC visibility push(default)

template <typename InputT, typename ContextT = mrc::runnable::Context>
class PythonSink : public node::RxSink<InputT, ContextT>,
                   public pymrc::AutoRegSinkAdapter<InputT>,
                   public pymrc::AutoRegEgressPort<InputT>
{
    using base_t = node::RxSink<InputT>;

  public:
    using typename base_t::observer_t;

    using node::RxSink<InputT>::RxSink;
};

template <typename InputT>
class PythonSinkComponent : public node::RxSinkComponent<InputT>,
                            public pymrc::AutoRegSinkAdapter<InputT>,
                            public pymrc::AutoRegEgressPort<InputT>
{
    using base_t = node::RxSinkComponent<InputT>;

  public:
    using typename base_t::observer_t;

    using base_t::RxSinkComponent;
};

template <typename InputT, typename OutputT, typename ContextT = mrc::runnable::Context>
class PythonNode : public node::RxNode<InputT, OutputT, ContextT>,
                   public pymrc::AutoRegSourceAdapter<OutputT>,
                   public pymrc::AutoRegSinkAdapter<InputT>,
                   public pymrc::AutoRegIngressPort<OutputT>,
                   public pymrc::AutoRegEgressPort<InputT>
{
    using base_t = node::RxNode<InputT, OutputT>;

  public:
    using typename base_t::stream_fn_t;
    using subscribe_fn_t = std::function<rxcpp::subscription(rxcpp::observable<InputT>, rxcpp::subscriber<OutputT>)>;

    using node::RxNode<InputT, OutputT>::RxNode;

  protected:
    static auto op_factory_from_sub_fn(subscribe_fn_t sub_fn)
    {
        return [=](rxcpp::observable<InputT> input) {
            // Convert from the `subscription(observable, subscriber)` signature into an operator factor function
            // `observable(observable)`
            return rxcpp::observable<>::create<OutputT>([=](rxcpp::subscriber<OutputT> output) {
                // Call the wrapped function
                sub_fn(input, output);
            });
        };
    }

  private:
    channel::Status no_channel(OutputT&& data)
    {
        if constexpr (pybind11::detail::is_pyobject<OutputT>::value)
        {
            pybind11::gil_scoped_acquire gil;
            OutputT tmp = std::move(data);
        }
        else
        {
            OutputT tmp = std::move(data);
        }

        return channel::Status::success;
    }
};

template <typename OutputT, typename ContextT = mrc::runnable::Context>
class PythonSource : public node::RxSource<OutputT, ContextT>,
                     public pymrc::AutoRegSourceAdapter<OutputT>,
                     public pymrc::AutoRegIngressPort<OutputT>
{
    using base_t = node::RxSource<OutputT>;

  public:
    using subscriber_fn_t = std::function<void(rxcpp::subscriber<OutputT>& sub)>;

    PythonSource(const subscriber_fn_t& f) :
      base_t(rxcpp::observable<>::create<OutputT>([f](rxcpp::subscriber<OutputT>& s) {
          // Call the wrapped subscriber function
          f(s);
      }))
    {}
};

class SegmentObjectProxy
{
    // add name
};

#pragma GCC visibility pop

}  // namespace pymrc
}  // namespace mrc