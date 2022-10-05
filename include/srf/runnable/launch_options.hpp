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

#include "srf/constants.hpp"
#include "srf/options/engine_groups.hpp"
#include "srf/runnable/types.hpp"

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <string>

namespace srf::runnable {

struct LaunchOptions
{
    LaunchOptions();

    LaunchOptions(std::string name, std::size_t num_pe = 1, std::size_t num_workers = 0);

    void set_counts(std::size_t num_pe, std::size_t num_workers = 0);

    std::size_t pe_count() const;

    std::size_t pe_per_worker() const;

    std::size_t worker_per_pe() const;

    // Deprecated property. Use pe_per_worker().
    std::size_t engines_per_pe() const;

    std::size_t worker_count() const;

    void set_engine_factory_name(const std::string& engine_factory_name);

    const std::string& engine_factory_name() const;

  protected:
    std::string m_engine_factory_name{default_engine_factory_name()};
    std::size_t m_pe_count{1};
    std::size_t m_worker_count{1};

    // Derived values
    std::size_t m_pe_per_worker{1};
    std::size_t m_worker_per_pe{1};
};

struct ServiceLaunchOptions : public LaunchOptions
{
    int m_priority{SRF_DEFAULT_FIBER_PRIORITY};
};

}  // namespace srf::runnable
