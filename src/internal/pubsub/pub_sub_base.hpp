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

#include "internal/control_plane/client/subscription_service.hpp"
#include "internal/resources/partition_resources.hpp"
#include "internal/runtime/runtime.hpp"

#include "srf/pubsub/client_subscription_base.hpp"
#include "srf/types.hpp"

#include <memory>
#include <string>

namespace srf::internal::pubsub {

/**
 * @brief PubSub is a specialization of the generic SubscriptionService
 *
 * This class defines the set of allowed roles.
 */
class PubSubBase : public control_plane::client::SubscriptionService
{
  public:
    using tagged_members_t = ::srf::pubsub::ClientSubscriptionBase::tagged_members_t;

    PubSubBase(std::shared_ptr<srf::pubsub::IService> client_subscription) :
      SubscriptionService(client_subscription, client_subscription->runtime().resources().network()->control_plane()),
      m_runtime(runtime)
    {}
    using SubscriptionService::SubscriptionService;

    static const std::string& role_publisher()
    {
        static std::string name = "publisher";
        return name;
    }

    static const std::string& role_subscriber()
    {
        static std::string name = "subscriber";
        return name;
    }

    const std::set<std::string>& roles() const final
    {
        static std::set<std::string> r = {role_publisher(), role_subscriber()};
        return r;
    }

  protected:
    inline runtime::Runtime& runtime()
    {
        return m_runtime;
    }

    inline resources::PartitionResources& resources()
    {
        return m_runtime.resources();
    }

    // const std::unordered_map<std::uint64_t, InstanceID>& tagged_instances() const
    // {
    //     return m_tagged_instances;
    // }

    void set_main_runner(std::unique_ptr<srf::runnable::Runner>&& main_runner)
    {
        main_runner->on_completion_callback([this](bool ok) {
            VLOG(10) << "Pub/Sub Service: '" << this->service_name() << "/" << this->role() << "' Runner completed.";

            // Tell the subscription that we are completed
            m_client_subscription->m_service_completed_promise.set_value();
        });

        m_main_runner = std::move(main_runner);
    }

    void update_tagged_members(::srf::pubsub::SubscriptionState state, const tagged_members_t& members) override
    {
        // m_tagged_instances.clear();

        // for (const auto& [instance_id, member] : members)
        // {
        //     m_tagged_instances[member.tag] = member.instance_id;
        // }

        m_client_subscription->update_tagged_members(state, members);
    }

    void do_service_await_live() override
    {
        m_main_runner->await_live();
    }

    void do_service_stop() override
    {
        m_main_runner->stop();
    }

    void do_service_kill() override
    {
        m_main_runner->kill();
    }

    void do_service_await_join() override
    {
        m_main_runner->await_join();
    }

  private:
    runtime::Runtime& m_runtime;
    std::unique_ptr<srf::runnable::Runner> m_main_runner;
    // std::shared_ptr<srf::pubsub::ClientSubscriptionBase> m_client_subscription;
    // std::unordered_map<TagID, InstanceID> m_tagged_instances;
};

}  // namespace srf::internal::pubsub
