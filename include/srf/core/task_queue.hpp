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

#include "srf/core/fiber_meta_data.hpp"
#include "srf/types.hpp"

#include <boost/fiber/all.hpp>
#include <boost/fiber/future/future.hpp>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <set>
#include <thread>

namespace srf::core {

class FiberTaskQueue
{
  protected:
    using task_t     = boost::fibers::packaged_task<void()>;
    using task_pkg_t = std::pair<task_t, FiberMetaData>;

  public:
    virtual ~FiberTaskQueue() = default;

    template <class F, class... ArgsT>
    auto enqueue(F&& f, ArgsT&&... args) -> Future<typename std::result_of<F(ArgsT...)>::type>
    {
        FiberMetaData meta_data;
        return enqueue(meta_data, std::forward<F>(f), std::forward<ArgsT>(args)...);
    }

    template <class F, class... ArgsT>
    auto enqueue(const FiberMetaData& meta_data, F&& f, ArgsT&&... args)
        -> Future<typename std::result_of<F(ArgsT...)>::type>
    {
        FiberMetaData copy = meta_data;
        return enqueue(std::move(copy), std::forward<F>(f), std::forward<ArgsT>(args)...);
    }

    template <class F, class... ArgsT>
    auto enqueue(FiberMetaData&& meta_data, F&& f, ArgsT&&... args)
        -> Future<typename std::result_of<F(ArgsT...)>::type>
    {
        if (task_queue().is_closed())
        {
            throw std::runtime_error("enqueue on stopped ws fiber pool");
        }

        using namespace boost::fibers;
        using return_type_t = typename std::result_of<F(ArgsT...)>::type;

        packaged_task<return_type_t()> task(std::bind(std::forward<F>(f), std::forward<ArgsT>(args)...));
        future<return_type_t> future = task.get_future();

        // auto fiber_count_id = ++m_count;

        // track detached fibers - main fiber will wait on all detached fibers to finish
        packaged_task<void()> wrapped_task([this, t = std::move(task), fiber_count_id]() mutable {
            // VLOG(10) << "===FiberCount===: Created " << fiber_count_id;
            t();
            --m_detached;
            // VLOG(10) << "===FiberCount===: Destroyed " << fiber_count_id;
            // m_outstanding_fiber_ids.erase(fiber_count_id);
        });

        // VLOG(10) << "===FiberCount===: Enqueuing " << fiber_count_id;
        ++m_detached;
        // m_outstanding_fiber_ids.insert(fiber_count_id);

        // package the wrapped task and meta data as a tuple and push to the buffered channel
        auto task_package = std::make_pair(std::move(wrapped_task), std::move(meta_data));

        // push the task package to the buffered channel
        task_queue().push(std::move(task_package));

        // return future
        return future;
    }

    virtual const CpuSet& affinity() const     = 0;
    virtual std::thread::id thread_id() const  = 0;
    virtual bool caller_on_same_thread() const = 0;

  protected:
    std::size_t detached() const
    {
        return m_detached.load();
    }

    // const std::set<std::size_t>& outstanding_fiber_ids() const
    // {
    //     return m_outstanding_fiber_ids;
    // }

  private:
    virtual boost::fibers::buffered_channel<task_pkg_t>& task_queue() = 0;

    // std::atomic<std::size_t> m_count{0};
    std::atomic<std::size_t> m_detached{0};
    // std::set<std::size_t> m_outstanding_fiber_ids;
};

}  // namespace srf::core
