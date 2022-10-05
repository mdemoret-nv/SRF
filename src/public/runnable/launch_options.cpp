/**
 * SPDX-FileCopyrightText: Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#include "srf/runnable/launch_options.hpp"

namespace srf::runnable {
LaunchOptions::LaunchOptions()
{
    this->set_counts(1, 0);
}

LaunchOptions::LaunchOptions(std::string name, std::size_t num_pe, std::size_t num_workers) :
  m_engine_factory_name(std::move(name))
{
    this->set_counts(num_pe, num_workers);
}

void LaunchOptions::set_counts(std::size_t num_pe, std::size_t num_workers)
{
    if (num_workers == 0)
    {
        // Default to 1 worker per PE if not specified
        num_workers = num_pe;
    }

    if (num_pe > num_workers)
    {
        CHECK_EQ(num_pe % num_workers, 0) << "If num_pe > num_workers, num_pe must be divisible by num_workers";

        this->m_pe_per_worker = num_pe / num_workers;
        this->m_worker_per_pe = 1;
    }
    else if (num_pe < num_workers)
    {
        CHECK_EQ(num_workers % num_pe, 0) << "If num_pe < num_workers, num_workers must be divisible by num_pe";

        this->m_pe_per_worker = 1;
        this->m_worker_per_pe = num_workers / num_pe;
    }
    else
    {
        // They are the same
        this->m_pe_per_worker = 1;
        this->m_worker_per_pe = 1;
    }

    this->m_pe_count     = num_pe;
    this->m_worker_count = num_workers;
}

std::size_t LaunchOptions::pe_count() const
{
    return this->m_pe_count;
}

std::size_t LaunchOptions::pe_per_worker() const
{
    return this->m_pe_per_worker;
}

std::size_t LaunchOptions::worker_per_pe() const
{
    return this->m_worker_per_pe;
}

std::size_t LaunchOptions::engines_per_pe() const
{
    return this->m_pe_per_worker;
}

std::size_t LaunchOptions::worker_count() const
{
    return this->m_worker_count;
}

void LaunchOptions::set_engine_factory_name(const std::string& engine_factory_name)
{
    this->m_engine_factory_name = engine_factory_name;
}

const std::string& LaunchOptions::engine_factory_name() const
{
    return this->m_engine_factory_name;
}
}  // namespace srf::runnable
