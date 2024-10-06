/*
 * SPDX-FileCopyrightText: Copyright (c) 2021-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#include "pymrc/utilities/object_wrappers.hpp"

#include "mrc/core/utils.hpp"
#include "mrc/types.hpp"  // for Future, SharedFuture

#include <boost/fiber/buffered_channel.hpp>
#include <boost/fiber/future/future.hpp>
#include <boost/fiber/future/promise.hpp>
#include <pybind11/pytypes.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>  // for future & promise
#include <memory>
#include <mutex>
#include <utility>

namespace mrc {
class Options;
}  // namespace mrc

namespace mrc::pipeline {
class IExecutor;
}

namespace mrc::pymrc {
class Pipeline;

// Export everything in the mrc::pymrc namespace by default since we compile with -fvisibility=hidden
#pragma GCC visibility push(default)

// This object serves as a bridge between awaiting in python and awaiting on fibers
class Awaitable : public std::enable_shared_from_this<Awaitable>
{
  public:
    Awaitable();

    Awaitable(Future<pybind11::object>&& _future);

    std::shared_ptr<Awaitable> iter();

    std::shared_ptr<Awaitable> await();

    std::shared_ptr<Awaitable> next();

    pybind11::object get_loop() const;

    void add_done_callback(pybind11::object callback, pybind11::object context);

    bool asyncio_future_blocking{false};

  private:
    Future<pybind11::object> m_future;

    boost::fibers::fiber m_schedule_fiber;

    PyObjectHolder m_loop;

    std::vector<std::tuple<PyObjectHolder, PyObjectHolder>> m_callbacks;
};

class PyAsyncPromise;

struct PySharedState
{
    pybind11::object get_result();

    // State m_state{State::Pending};
    bool m_is_ready{false};

    std::mutex m_mutex;
    std::condition_variable m_cv;

    // PyObjectHolder m_loop;

    PyObjectHolder m_result;
    std::exception_ptr m_exception;
};

class PyAsyncFuture : public std::enable_shared_from_this<PyAsyncFuture>
{
  public:
    bool asyncio_future_blocking{true};

    // Future interface
    std::shared_ptr<PyAsyncFuture> iter();

    std::shared_ptr<PyAsyncFuture> await();

    std::shared_ptr<PyAsyncFuture> next();

    pybind11::object result();

    pybind11::object exception();

    pybind11::object get_loop() const;

    void add_done_callback(pybind11::object callback, pybind11::object context);

  private:
    PyAsyncFuture(std::shared_ptr<PyAsyncPromise> parent);

    std::shared_ptr<PyAsyncPromise> m_parent;

    PyObjectHolder m_loop;

    friend class PyAsyncPromise;
};

class PyAsyncPromise : public std::enable_shared_from_this<PyAsyncPromise>
{
  public:
    enum class State
    {
        Pending = 0,
        Cancelled,
        Finished
    };

    PyAsyncPromise();

    /**
     * @brief Gets a future object which can be awaited on in python using Asyncio. Requires a running loop on the
     * current thread.
     *
     * @return std::shared_ptr<PyAsyncFuture>
     */
    std::shared_ptr<PyAsyncFuture> await();

    void add_done_callback(std::function<void()> callback);

    pybind11::object result();

    pybind11::object exception();

    void set_result(pybind11::object&& obj);

    void set_exception(std::exception_ptr&& e);

  private:
    pybind11::object get_result(std::unique_lock<std::mutex>& lock);

    void schedule_callbacks();

    State m_state{State::Pending};

    std::mutex m_mutex;
    std::condition_variable m_cv;

    PyObjectHolder m_result;
    std::exception_ptr m_exception;

    std::vector<std::function<void()>> m_callbacks;

    friend class PyAsyncFuture;
};

class Executor
{
  public:
    Executor();
    Executor(std::shared_ptr<Options> options);
    ~Executor();

    void register_pipeline(pymrc::Pipeline& pipeline);

    void start();
    void stop();
    void join();
    std::shared_ptr<PyAsyncPromise> join_async();

    std::shared_ptr<pipeline::IExecutor> get_executor() const;

  private:
    template <class F, class... ArgsT>
    auto enqueue_work(F&& f, ArgsT&&... args) -> Future<std::result_of_t<F(ArgsT...)>>
    {
        using namespace boost::fibers;
        using return_type_t = std::result_of_t<F(ArgsT...)>;

        packaged_task<return_type_t()> task(std::bind(std::forward<F>(f), std::forward<ArgsT>(args)...));
        future<return_type_t> future = task.get_future();

        // track detached fibers - main fiber will wait on all detached fibers to finish
        packaged_task<void()> wrapped_task([this, t = std::move(task)]() mutable {
            auto decrement_detached = Unwinder::create([this]() {
                --m_pending_work_count;
            });

            // Call the task
            t();

            // Detached will get automatically decremented even if there is an exception
        });
        ++m_pending_work_count;

        m_work_queue.push(std::move(wrapped_task));

        return future;
    }

    std::shared_ptr<PyAsyncPromise> m_join_promise;
    SharedFuture<void> m_join_future;
    // std::shared_future<void> m_join_future2;

    std::mutex m_mutex;
    std::thread m_join_thread;
    // std::vector<std::shared_ptr<PyBoostPromise>> m_join_promises;

    std::atomic_int m_pending_work_count{0};
    boost::fibers::buffered_channel<boost::fibers::packaged_task<void()>> m_work_queue;

    std::shared_ptr<pipeline::IExecutor> m_exec;
};

#pragma GCC visibility pop

}  // namespace mrc::pymrc
