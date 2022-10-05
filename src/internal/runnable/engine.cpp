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

#include "internal/runnable/engine.hpp"

#include "srf/types.hpp"

#include <glog/logging.h>

#include <mutex>
#include <ostream>
#include <utility>

namespace srf::internal::runnable {

Engine::Engine(std::size_t worker_count) : m_worker_count(worker_count)
{
    CHECK_GT(m_worker_count, 0) << "worker_count must be non-zero";
}

std::size_t Engine::worker_count() const
{
    return m_worker_count;
}

Future<void> Engine::run_task(std::function<void()> task)
{
    return this->run_task(m_current_worker_idx++ % m_worker_count, std::move(task));
}

Future<void> Engine::run_task(std::size_t worker_idx, std::function<void()> task)
{
    std::lock_guard<decltype(m_mutex)> lock(m_mutex);
    if (!m_launched)
    {
        LOG(FATAL) << "Cannot call run_task until the engine has been started";
    }

    CHECK(worker_idx < m_worker_count) << "worker_idx must be less than worker_count";

    return this->do_launch_task(worker_idx, std::move(task));
}

Future<void> Engine::launch_task(std::function<void()> task)
{
    std::lock_guard<decltype(m_mutex)> lock(m_mutex);
    if (m_launched)
    {
        LOG(FATAL) << "detected attempted reuse of a runnable::Engine; this is a fatal error";
    }
    m_launched = true;
    return do_launch_task(m_current_worker_idx++ % m_worker_count, std::move(task));
}

}  // namespace srf::internal::runnable
