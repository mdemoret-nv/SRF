/**
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#include "internal/async_service.hpp"
#include "internal/control_plane/client.hpp"
#include "internal/resources/partition_resources.hpp"
#include "internal/resources/partition_resources_base.hpp"
#include "internal/ucx/resources.hpp"

#include "mrc/types.hpp"

#include <cstddef>
#include <optional>

namespace mrc::internal::memory {
class DeviceResources;
class HostResources;
}  // namespace mrc::internal::memory
namespace mrc::internal::network {
class NetworkResources;
}  // namespace mrc::internal::network
namespace mrc::internal::runnable {
class RunnableResources;
}  // namespace mrc::internal::runnable

namespace mrc::internal::runtime {

/**
 * @brief Partition Resources define the set of Resources available to a given Partition
 *
 * This class does not own the actual resources, that honor is bestowed on the resources::Manager. This class is
 * constructed and owned by the resources::Manager to ensure validity of the references.
 */
class PartitionManager : public AsyncService
{
  public:
    PartitionManager(resources::PartitionResources& resources, control_plane::Client& control_plane_client);
    ~PartitionManager() override;
    // PartitionManager(runnable::RunnableResources& runnable_resources,
    //                  std::size_t partition_id,
    //                  memory::HostResources& host,
    //                  std::optional<memory::DeviceResources>& device,
    //                  std::optional<network::NetworkResources>& network);

    // memory::HostResources& host();
    // std::optional<memory::DeviceResources>& device();
    // std::optional<network::NetworkResources>& network();

  private:
    void do_service_start(std::stop_token stop_token) final;
    // void do_service_stop() final;
    // void do_service_kill() final;
    // void do_service_await_live() final;
    // void do_service_await_join() final;

    void process_state_update(mrc::protos::Worker& worker);

    resources::PartitionResources& m_resources;
    control_plane::Client& m_control_plane_client;

    InstanceID m_instance_id{0};

    Future<void> m_shutdown_future;
    SharedPromise<void> m_live_promise;

    // memory::HostResources& m_host;
    // std::optional<memory::DeviceResources>& m_device;
    // std::optional<network::NetworkResources>& m_network;
};

}  // namespace mrc::internal::runtime