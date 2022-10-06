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

#include "pysrf/observable.hpp"

#include "pysrf/operators.hpp"
#include "pysrf/types.hpp"

#include "srf/runnable/rx_thread_factory.hpp"

#include <glog/logging.h>
#include <pybind11/cast.h>
#include <pybind11/eval.h>
#include <pybind11/gil.h>
#include <pybind11/pybind11.h>  // IWYU pragma: keep
#include <pybind11/pytypes.h>
#include <rxcpp/rx.hpp>

#include <array>
#include <exception>
#include <functional>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace srf::pysrf {

namespace py = pybind11;
using namespace py::literals;

std::function<void(PyObjectSubscriber&)> source_iterate(std::function<py::iterator()> iter_factory)
{
    auto wrapper = [iter_factory](PyObjectSubscriber& subscriber) mutable {
        auto& ctx = runnable::Context::get_runtime_context();

        AcquireGIL gil;

        try
        {
            DVLOG(10) << ctx.info() << " Starting source";

            // Get the iterator from the factory
            auto iter = iter_factory();

            // See if this is a generator
            bool is_generator = PyGen_Check(iter.ptr());

            // Loop over the iterator
            while (subscriber.is_subscribed())
            {
                // Check to see if we are out of values
                if (iter == py::iterator::sentinel())
                {
                    break;
                }

                // Get the next value
                auto next_val = py::cast<py::object>(*iter);

                // Increment it for next loop
                ++iter;

                {
                    // Release the GIL to call on_next
                    pybind11::gil_scoped_release nogil;

                    //  Only send if its subscribed. Very important to ensure the object has been moved!
                    if (subscriber.is_subscribed())
                    {
                        subscriber.on_next(std::move(next_val));
                    }
                }
            }

            if (is_generator)
            {
                // Instruct the generator to close (incase we are shutting down early)
                iter.attr("close")();
            }

        } catch (const std::exception& e)
        {
            LOG(ERROR) << ctx.info() << "Error occurred in source. Error msg: " << e.what();

            gil.release();
            subscriber.on_error(std::current_exception());
            return;
        }

        // Release the GIL to call on_complete
        gil.release();

        subscriber.on_completed();

        DVLOG(10) << ctx.info() << " Source complete";
    };

    return wrapper;
}

PyObjectObservable ObservableProxy::iterate(pybind11::iterable source_iterable)
{
    // Capture the iterator
    return rxcpp::observable<>::create<PyHolder>(
        source_iterate([iterable = PyObjectHolder(std::move(source_iterable))]() {
            // Turn the iterable into an iterator
            return py::iter(iterable);
        }));
}

PyObjectObservable ObservableProxy::create(pybind11::function generator_fn, bool observe_on_new_thread)
{
    auto obs =
        rxcpp::observable<>::create<PyHolder>(source_iterate([gen_fn = PyObjectHolder(std::move(generator_fn))]() {
            // Turn the generator function into an iterator
            return py::iter(gen_fn());
        }));

    if (observe_on_new_thread)
    {
        return obs.subscribe_on(srf::runnable::observe_on_new_srf_thread()).as_dynamic();
    }

    return obs;
}

PySubscription ObservableProxy::subscribe(PyObjectObservable* self, PyObjectObserver& observer)
{
    // Call the internal subscribe function
    return self->subscribe(observer);
}

PySubscription ObservableProxy::subscribe(PyObjectObservable* self, PyObjectSubscriber& subscriber)
{
    // Call the internal subscribe function
    return self->subscribe(subscriber);
}

std::function<PyObjectObservable(PyObjectObservable&)> test_operator()
{
    return [](PyObjectObservable& source) {
        return source.tap([](auto x) {
            // Print stuff
        });
    };
}

template <typename... OpsT>
PyObjectObservable pipe_ops(PyObjectObservable* self, OpsT&&... ops)
{
    return (*self | ... | ops);
}

PyObjectObservable ObservableProxy::pipe(PyObjectObservable* self, py::args args)
{
    std::vector<PyObjectOperateFn> operators;

    for (const auto& a : args)
    {
        if (!py::isinstance<PythonOperator>(a))
        {
            throw std::runtime_error("All arguments must be Operators");
        }

        auto op = a.cast<PythonOperator>();

        operators.emplace_back(op.get_operate_fn());
    }

    switch (operators.size())
    {
    case 1:
        return pipe_ops(self, operators[0]);
    case 2:
        return pipe_ops(self, operators[0], operators[1]);
    case 3:
        return pipe_ops(self, operators[0], operators[1], operators[2]);
    case 4:
        return pipe_ops(self, operators[0], operators[1], operators[2], operators[3]);
    case 5:
        return pipe_ops(self, operators[0], operators[1], operators[2], operators[3], operators[4]);
    case 6:
        return pipe_ops(self, operators[0], operators[1], operators[2], operators[3], operators[4], operators[5]);
    case 7:
        return pipe_ops(
            self, operators[0], operators[1], operators[2], operators[3], operators[4], operators[5], operators[6]);
    case 8:
        return pipe_ops(self,
                        operators[0],
                        operators[1],
                        operators[2],
                        operators[3],
                        operators[4],
                        operators[5],
                        operators[6],
                        operators[7]);
    case 9:
        return pipe_ops(self,
                        operators[0],
                        operators[1],
                        operators[2],
                        operators[3],
                        operators[4],
                        operators[5],
                        operators[6],
                        operators[7],
                        operators[8]);
    case 10:
        return pipe_ops(self,
                        operators[0],
                        operators[1],
                        operators[2],
                        operators[3],
                        operators[4],
                        operators[5],
                        operators[6],
                        operators[7],
                        operators[8],
                        operators[9]);
    default:
        // Not supported error
        throw std::runtime_error("pipe() only supports up 10 arguments. Please use another pipe() to use more");
    }
}

}  // namespace srf::pysrf
