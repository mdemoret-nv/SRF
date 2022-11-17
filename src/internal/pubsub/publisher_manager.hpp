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

#include "internal/control_plane/client.hpp"
#include "internal/control_plane/client/instance.hpp"
#include "internal/control_plane/client/subscription_service.hpp"
#include "internal/control_plane/server/subscription_manager.hpp"
#include "internal/data_plane/client.hpp"
#include "internal/data_plane/request.hpp"
#include "internal/expected.hpp"
#include "internal/network/resources.hpp"
#include "internal/pubsub/pub_sub_base.hpp"
#include "internal/resources/forward.hpp"
#include "internal/resources/partition_resources.hpp"
#include "internal/runtime/runtime.hpp"
#include "internal/service.hpp"
#include "internal/ucx/common.hpp"
#include "internal/utils/contains.hpp"

#include "srf/channel/channel.hpp"
#include "srf/channel/ingress.hpp"
#include "srf/channel/status.hpp"
#include "srf/codable/encode.hpp"
#include "srf/codable/encoded_object.hpp"
#include "srf/node/edge_builder.hpp"
#include "srf/node/queue.hpp"
#include "srf/node/rx_sink.hpp"
#include "srf/node/source_channel.hpp"
#include "srf/node/source_properties.hpp"
#include "srf/protos/architect.pb.h"
#include "srf/pubsub/publisher.hpp"
#include "srf/pubsub/state.hpp"
#include "srf/utils/macros.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

namespace srf::internal::pubsub {

template <typename InputT, typename OutputT>
class LambdaNodeComponent : public node::SinkProperties<InputT>, public node::SourceChannel<OutputT>
{
  public:
    using on_data_fn_t = std::function<OutputT(InputT&&)>;

    LambdaNodeComponent(on_data_fn_t on_data_fn) : m_on_data_fn(std::move(on_data_fn)) {}

  private:
    struct Upstream : channel::Ingress<InputT>
    {
        Upstream(LambdaNodeComponent& parent) : m_parent(parent) {}

        ~Upstream() override
        {
            m_parent.release_channel();
        }

        channel::Status await_write(InputT&& data) override
        {
            return m_parent.await_write(m_parent.m_on_data_fn(std::move(data)));
        }

      private:
        LambdaNodeComponent& m_parent;
    };

    std::shared_ptr<channel::Ingress<InputT>> channel_ingress() override
    {
        if (is_weak_ptr_null(m_upstream))
        {
            // Create and return
            auto upstream = std::make_shared<Upstream>(*this);

            m_upstream = upstream;

            return upstream;
        }

        // Been created before, try to lock
        if (auto upstream = m_upstream.lock())
        {
            return upstream;
        }

        LOG(FATAL) << "Cannot get channel_ingress. Ingress has already been destroyed.";
    }

    std::weak_ptr<Upstream> m_upstream;
    on_data_fn_t m_on_data_fn;
};

template <typename T>
class LambdaSinkComponent : public node::SinkProperties<T>
{
  public:
    using on_data_fn_t = std::function<void(T&&)>;

    LambdaSinkComponent(on_data_fn_t on_data_fn) : m_on_data_fn(std::move(on_data_fn)) {}

  private:
    struct Upstream : channel::Ingress<T>
    {
        Upstream(LambdaSinkComponent& parent) : m_parent(parent) {}

        ~Upstream() override = default;

        channel::Status await_write(T&& data) override
        {
            m_parent.m_on_data_fn(std::move(data));

            return channel::Status::success;
        }

      private:
        LambdaSinkComponent& m_parent;
    };

    std::shared_ptr<channel::Ingress<T>> channel_ingress() override
    {
        if (is_weak_ptr_null(m_upstream))
        {
            // Create and return
            auto upstream = std::make_shared<Upstream>(*this);

            m_upstream = upstream;

            return upstream;
        }

        // Been created before, try to lock
        if (auto upstream = m_upstream.lock())
        {
            return upstream;
        }

        LOG(FATAL) << "Cannot get channel_ingress. Ingress has already been destroyed.";
    }

    std::weak_ptr<Upstream> m_upstream;
    on_data_fn_t m_on_data_fn;
};

class InternalPublisher : ::srf::pubsub::IPublisher
{};

class PublisherManager : public PubSubBase
{
  public:
    PublisherManager(std::string name, runtime::Runtime& runtime) : PubSubBase(publisher, runtime) {}

    ~PublisherManager() override
    {
        service_await_join();
    }

    const std::string& role() const final
    {
        return role_publisher();
    }

    const std::set<std::string>& subscribe_to_roles() const final
    {
        static std::set<std::string> r = {role_subscriber()};
        return r;
    }

    // Future<std::shared_ptr<Publisher<T>>> make_publisher()
    // {
    //     return m_publisher_promise.get_future();
    // }
    // auto get_drop_service_fn() const
    // {
    //     return this->drop_subscription_service();
    // }

  protected:
    const std::unordered_map<std::uint64_t, std::shared_ptr<ucx::Endpoint>>& tagged_endpoints() const
    {
        return m_tagged_endpoints;
    }

  private:
    // virtual void write(T&& object) = 0;
    // virtual void on_update()       = 0;

    void update_tagged_members(
        ::srf::pubsub::SubscriptionState state,
        const std::unordered_map<std::uint64_t, ::srf::pubsub::SubscriptionMember>& members) final
    {
        // todo - convert tagged instances -> tagged endpoints
        for (const auto& [tag_id, member] : members)
        {
            if (member.state == ::srf::pubsub::SubscriptionState::Connected)
            {
                if (!contains(m_tagged_endpoints, member.tag))
                {
                    m_tagged_endpoints[member.tag] =
                        resources().network()->data_plane().client().endpoint_shared(member.instance_id);
                }
            }
            else
            {
                // Need to remove the tagged endpoint
                m_tagged_endpoints.erase(member.tag);
            }
        }

        PubSubBase::update_tagged_members(state, members);
    }

    void handle_network_message(std::uint64_t id, std::unique_ptr<srf::remote_descriptor::Storage> data)
    {
        LOG(INFO) << "publisher writing object";

        DCHECK(this->runtime().runnable().main().caller_on_same_thread());

        auto found = m_tagged_endpoints.find(id);

        CHECK(found != m_tagged_endpoints.end()) << "Tagged ID must be in the list of available instances";

        internal::data_plane::RemoteDescriptorMessage msg;

        msg.tag = id;

        // TODO(MDD): Figure out a better way to get the endpoint
        msg.endpoint = found->second;
        msg.rd       = this->runtime().remote_descriptor_manager().store_object(std::move(data));

        CHECK(this->runtime().resources().network()->data_plane().client().remote_descriptor_channel().await_write(
                  std::move(msg)) == channel::Status::success);
    }

    void do_service_start() override
    {
        SubscriptionService::do_service_start();

        CHECK(this->tag() != 0);

        using incoming_t = std::pair<std::uint64_t, std::unique_ptr<srf::remote_descriptor::Storage>>;

        // TODO(MDD): Eventually, we should just make this runnable here. Need EgressAcceptor
        // m_sink = std::make_unique<LambdaSinkComponent<incoming_t>>([this](incoming_t&& data) {
        //     this->handle_network_message(std::get<0>(data), std::move(std::get<1>(data)));
        // });

        auto sink = std::make_unique<node::RxSink<incoming_t>>(
            [this](incoming_t data) {
                // Push the message to the network
                this->handle_network_message(std::get<0>(data), std::move(std::get<1>(data)));
            },
            [drop_service_fn = this->drop_subscription_service()]() {
                // Handle on_complete logic
                drop_service_fn();
            });

        auto publisher = dynamic_cast<node::SourceChannelWriteable<std::unique_ptr<srf::remote_descriptor::Storage>>&>(
            this->service());

        // Make an edge between IPublisher and the sink
        node::make_edge(publisher, *sink);

        auto launch_options = resources().network()->control_plane().client().launch_options();

        this->set_main_runner(
            resources().runnable().launch_control().prepare_launcher(launch_options, std::move(sink))->ignition());

        // // Now that the service has started, link the service to the publisher
        // this->set_main_runner(m_publisher->link_service(this->tag(),
        //                                                 this->drop_subscription_service(),
        //                                                 resources().runnable().launch_control(),
        //                                                 launch_options,
        //                                                 *m_sink));

        // auto drop_subscription_service_lambda = drop_subscription_service();

        // auto publisher = std::shared_ptr<Publisher<T>>(new Publisher<T>(service_name(), this->tag()),
        //                                                [drop_subscription_service_lambda](Publisher<T>* ptr) {
        //                                                    drop_subscription_service_lambda();
        //                                                    delete ptr;
        //                                                });
        // auto sink      = std::make_unique<srf::node::RxSink<T>>([this](T data) { write(std::move(data)); });
        // srf::node::make_edge(*publisher, *sink);

        // auto launch_options = resources().network()->control_plane().client().launch_options();

        // m_writer =
        //     resources().runnable().launch_control().prepare_launcher(launch_options, std::move(sink))->ignition();

        // m_publisher = publisher;
        // m_publisher_promise.set_value(std::move(publisher));

        // m_writer = m_publisher->launch(resources().runnable().launch_control(), launch_options);

        SRF_THROW_ON_ERROR(activate_subscription_service());
    }

    // void do_service_await_live() override
    // {
    //     m_writer->await_live();
    // }

    // void do_service_stop() override
    // {
    //     m_writer->stop();
    // }

    // void do_service_kill() override
    // {
    //     m_writer->kill();
    // }

    // void do_service_await_join() override
    // {
    //     m_writer->await_join();
    // }

    std::shared_ptr<srf::pubsub::PublisherBase> m_publisher;
    std::unique_ptr<LambdaSinkComponent<std::pair<std::uint64_t, std::unique_ptr<srf::remote_descriptor::Storage>>>>
        m_sink;  // This is a runner created by the publisher. Could be combined into one in the future
    // std::unique_ptr<srf::runnable::Runner> m_writer;
    // std::unordered_map<std::uint64_t, InstanceID> m_tagged_instances;
    std::unordered_map<std::uint64_t, std::shared_ptr<ucx::Endpoint>> m_tagged_endpoints;
    std::unordered_map<std::uint64_t, InstanceID>::const_iterator m_next;
    // Promise<std::shared_ptr<Publisher<T>>> m_publisher_promise;

    // friend std::function<void()> make_pub_service(std::unique_ptr<srf::pubsub::PublisherBase> publisher,
    //                                               core::IRuntime& runtime);
};

// template <typename T>
// class PublisherRoundRobin : public PublisherManager<T>
// {
//   public:
//     using PublisherManager<T>::PublisherManager;

//   private:
//     void on_update() final
//     {
//         m_next = this->tagged_endpoints().cbegin();
//     }

//     void write(T&& object) final
//     {
//         LOG(INFO) << "publisher writing object";

//         DCHECK(this->resources().runnable().main().caller_on_same_thread());

//         while (this->tagged_instances().empty())
//         {
//             // await subscribers
//             // for now just return and drop the object
//             boost::this_fiber::yield();
//         }

//         data_plane::RemoteDescriptorMessage msg;

//         msg.tag      = m_next->first;
//         msg.endpoint = m_next->second;

//         if (++m_next == this->tagged_endpoints().cend())
//         {
//             m_next = this->tagged_endpoints().cbegin();
//         }

//         msg.rd = this->runtime().remote_descriptor_manager().register_object(std::move(object));
//         CHECK(this->resources().network()->data_plane().client().remote_descriptor_channel().await_write(
//                   std::move(msg)) == channel::Status::success);
//     }

//     std::unordered_map<std::uint64_t, std::shared_ptr<ucx::Endpoint>>::const_iterator m_next;
// };

enum class PublisherType
{
    RoundRobin,
};

// template <typename T>
// std::shared_ptr<Publisher<T>> make_publisher(const std::string& name, PublisherType type, runtime::Runtime& runtime)
// {
//     std::unique_ptr<PublisherManager<T>> manager;
//     switch (type)
//     {
//     case PublisherType::RoundRobin:
//         manager = std::make_unique<PublisherRoundRobin<T>>(name, runtime);
//         break;
//     default:
//         LOG(FATAL) << "unknown publisher type";
//     }
//     CHECK(manager);

//     auto future = manager->make_publisher();
//     runtime.resources().network()->control_plane().register_subscription_service(std::move(manager));
//     return future.get();
// }

// void make_pub_service(std::shared_ptr<PublisherBase> publisher, runtime::Runtime& runtime)
// {
//     // std::unique_ptr<PublisherManager<T>> manager;
//     // switch (type)
//     // {
//     // case PublisherType::RoundRobin:
//     //     manager = std::make_unique<PublisherRoundRobin<T>>(name, runtime);
//     //     break;
//     // default:
//     //     LOG(FATAL) << "unknown publisher type";
//     // }
//     // CHECK(manager);

//     std::unique_ptr<PublisherManager> manager = std::make_unique<PublisherManager>(publisher, runtime);

//     // auto future = manager->make_publisher();
//     runtime.resources().network()->control_plane().register_subscription_service(std::move(manager));
//     // return future.get();
// }

}  // namespace srf::internal::pubsub
