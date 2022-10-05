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

#include "internal/runnable/fiber_engine.hpp"

#include "internal/runnable/engine.hpp"

#include "srf/core/fiber_meta_data.hpp"
#include "srf/core/task_queue.hpp"
#include "srf/runnable/types.hpp"
#include "srf/types.hpp"

#include <boost/fiber/future/future.hpp>

#include <type_traits>
#include <utility>

namespace srf::internal::runnable {

FiberEngine::FiberEngine(std::vector<std::reference_wrapper<core::FiberTaskQueue>>&& task_queues, int priority) :
  Engine(task_queues.size()),
  m_task_queues(std::move(task_queues)),
  m_meta{priority}
{}

FiberEngine::FiberEngine(std::vector<std::reference_wrapper<core::FiberTaskQueue>>&& task_queues,
                         const FiberMetaData& meta) :
  Engine(task_queues.size()),
  m_task_queues(std::move(task_queues)),
  m_meta(meta)
{}

Future<void> FiberEngine::do_launch_task(std::size_t worker_idx, std::function<void()> task)
{
    return m_task_queues[worker_idx].get().enqueue(m_meta, std::move(task));
}

runnable::EngineType FiberEngine::engine_type() const
{
    return EngineType::Fiber;
}
}  // namespace srf::internal::runnable
