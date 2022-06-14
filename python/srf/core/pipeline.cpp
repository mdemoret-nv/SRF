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

#include <memory>
#include <pysrf/pipeline.hpp>
#include <pysrf/segment.hpp>
#include <pysrf/utils.hpp>
#include <stdexcept>
#include <utility>

#include <pybind11/detail/typeid.h>
#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>
// IWYU thinks we need array for py::class_<Pipeline>
// IWYU pragma: no_include <array>

namespace srf::pysrf {

namespace py = pybind11;

class IngressPort
{
  public:
    IngressPort(std::string name, py::type t) : m_name(std::move(name)), m_type(std::move(t)) {}

  private:
    std::string m_name;
    py::type m_type;
};

class IngressPorts
{
  public:
    IngressPorts(std::vector<std::shared_ptr<IngressPort>> ports) : m_ports(std::move(ports)) {}

  private:
    std::vector<std::shared_ptr<IngressPort>> m_ports;
};

// Define the pybind11 module m, as 'pipeline'.
PYBIND11_MODULE(pipeline, m)
{
    pysrf::import(m, "srf.core.segment");

    m.doc() = R"pbdoc(
        SRF Pipeline interface bindings
        -----------------------
        .. currentmodule:: srf.pipeline
    )pbdoc";

    m.attr("SRF_MAX_INGRESS_PORTS") = SRF_MAX_INGRESS_PORTS;
    m.attr("SRF_MAX_EGRESS_PORTS")  = SRF_MAX_EGRESS_PORTS;

    py::class_<IngressPorts, std::shared_ptr<IngressPorts>>(m, "IngressPorts").def(py::init<>([](py::args args) {
        std::vector<std::shared_ptr<IngressPort>> ports;

        for (const auto& arg : args)
        {
            // Cast to a tuple
            py::tuple a_tuple = arg.cast<py::tuple>();

            ports.push_back(std::make_shared<IngressPort>(a_tuple[0].cast<std::string>(), a_tuple[1]));
        }

        // Create an insteance
        return std::make_shared<IngressPorts>(ports);
    }));

    // py::class_<IngressPort>(m, "IngressPort").def(py::init<>([](std::string name, py::object t) {
    //     // Create an insteance
    //     return std::make_shared<IngressPort>(name, t);
    // }));

    py::class_<Pipeline>(m, "Pipeline")
        .def(py::init<>())
        .def(
            "make_segment",
            wrap_segment_init_callback(
                static_cast<void (Pipeline::*)(const std::string&, const std::function<void(srf::segment::Builder&)>&)>(
                    &Pipeline::make_segment)))
        .def("make_segment",
             wrap_segment_init_callback(
                 static_cast<void (Pipeline::*)(const std::string&,
                                                const std::vector<std::string>&,
                                                const std::vector<std::string>&,
                                                const std::function<void(srf::segment::Builder&)>&)>(
                     &Pipeline::make_segment)))
        .def("test_types", [](Pipeline& self, py::list type_object) {
            py::type py_type = type_object;

            // Now look for this as a registered type
            auto type_info = py::detail::get_type_info((PyTypeObject*)py_type.ptr());

            if (type_info == nullptr)
            {
                throw std::runtime_error("Unknown type");
            }

            py::print("Printing the type:");
            py::print(py_type);
        });

#ifdef VERSION_INFO
    m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
    m.attr("__version__") = "dev";
#endif
}
}  // namespace srf::pysrf
