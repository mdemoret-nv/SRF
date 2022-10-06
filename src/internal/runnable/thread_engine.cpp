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

#include "internal/runnable/thread_engine.hpp"

#include "internal/system/resources.hpp"

#include "srf/core/bitmap.hpp"
#include "srf/runnable/types.hpp"
#include "srf/types.hpp"

#include <boost/fiber/future/future.hpp>
#include <boost/fiber/future/packaged_task.hpp>

#include <optional>
#include <type_traits>
#include <utility>

namespace srf::internal::runnable {

ThreadEngine::ThreadEngine(CpuSet cpu_set, const system::Resources& system) :
  Engine(cpu_set.weight()),
  m_cpu_set(std::move(cpu_set)),
  m_system(system)
{}

ThreadEngine::~ThreadEngine() = default;

std::optional<std::thread::id> ThreadEngine::get_id() const
{
    if (m_thread)
    {
        return m_thread->thread().get_id();
    }
    return std::nullopt;
}

Future<void> ThreadEngine::do_launch_task(std::size_t worker_idx, std::function<void()> task)
{
    auto final_cpu_set = m_cpu_set.next_binding();

    boost::fibers::packaged_task<void()> pkg_task(std::move(task));
    auto future = pkg_task.get_future();

    auto thread =
        std::make_unique<system::Thread>(m_system.make_thread("thread_engine", final_cpu_set, std::move(pkg_task)));

    if (!m_thread)
    {
        // Only on the first call do we save the thread
        m_thread = std::move(thread);
    }
    else
    {
        // Otherwise detach to prevent immediate cleanup
        thread->detach();
    }

    return std::move(future);
}

runnable::EngineType ThreadEngine::engine_type() const
{
    return EngineType::Thread;
}

}  // namespace srf::internal::runnable
