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

#include "srf/runnable/engine.hpp"
#include "srf/runnable/types.hpp"
#include "srf/types.hpp"

#include <atomic>
#include <cstddef>
#include <functional>
#include <mutex>

namespace srf::internal::runnable {

using ::srf::runnable::EngineType;

class Engine : public ::srf::runnable::Engine
{
  public:
    std::size_t worker_count() const override;

    Future<void> run_task(std::function<void()> task) override;
    Future<void> run_task(std::size_t worker_idx, std::function<void()> task) override;

  protected:
    Engine(std::size_t worker_count);

  private:
    Future<void> launch_task(std::function<void()> task) final;

    virtual Future<void> do_launch_task(std::size_t worker_idx, std::function<void()> task) = 0;

    bool m_launched{false};
    std::size_t m_worker_count{0};
    std::atomic_size_t m_current_worker_idx{0};
    std::mutex m_mutex;
};

}  // namespace srf::internal::runnable
