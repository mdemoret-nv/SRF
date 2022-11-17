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

#include "rxcpp/operators/rx-map.hpp"

#include "internal/control_plane/client.hpp"
#include "internal/control_plane/client/instance.hpp"
#include "internal/control_plane/client/subscription_service.hpp"
#include "internal/control_plane/server/subscription_manager.hpp"
#include "internal/data_plane/request.hpp"
#include "internal/data_plane/server.hpp"
#include "internal/expected.hpp"
#include "internal/memory/transient_pool.hpp"
#include "internal/network/resources.hpp"
#include "internal/pubsub/pub_sub_base.hpp"
#include "internal/remote_descriptor/encoded_object.hpp"
#include "internal/remote_descriptor/manager.hpp"
#include "internal/remote_descriptor/remote_descriptor.hpp"
#include "internal/resources/forward.hpp"
#include "internal/resources/partition_resources.hpp"
#include "internal/runtime/runtime.hpp"
#include "internal/service.hpp"

#include "srf/channel/channel.hpp"
#include "srf/channel/ingress.hpp"
#include "srf/channel/status.hpp"
#include "srf/codable/decode.hpp"
#include "srf/codable/encode.hpp"
#include "srf/codable/encoded_object.hpp"
#include "srf/node/edge_builder.hpp"
#include "srf/node/operators/router.hpp"
#include "srf/node/queue.hpp"
#include "srf/node/rx_sink.hpp"
#include "srf/node/sink_channel.hpp"
#include "srf/node/sink_properties.hpp"
#include "srf/node/source_channel.hpp"
#include "srf/node/source_properties.hpp"
#include "srf/protos/architect.pb.h"
#include "srf/protos/codable.pb.h"
#include "srf/pubsub/state.hpp"
#include "srf/pubsub/subscriber.hpp"
#include "srf/utils/bytes_to_string.hpp"
#include "srf/utils/macros.hpp"

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace srf::internal::pubsub {

class SubscriberParser : public node::SinkProperties<memory::TransientBuffer>,
                         public node::SourceChannel<std::unique_ptr<codable::EncodedObject>>
{
  public:
    SubscriberParser(runtime::Runtime& runtime) : m_runtime(runtime) {}

  private:
    struct ParserIngress : channel::Ingress<memory::TransientBuffer>
    {
        ParserIngress(SubscriberParser& parent) : m_parent(parent) {}

        ~ParserIngress() override = default;

        channel::Status await_write(memory::TransientBuffer&& buffer) override
        {
            // deserialize remote descriptor handle/proto from transient buffer
            auto handle = std::make_unique<srf::codable::protos::RemoteDescriptor>();
            CHECK(handle->ParseFromArray(buffer.data(), buffer.bytes()));

            LOG(INFO) << "transient buffer holding the rd: " << srf::bytes_to_string(buffer.bytes());

            // release transient buffer so it can be reused
            buffer.release();

            // create a remote descriptor via the local RD manager taking ownership of the handle
            // auto encoded_object = m_parent.m_manager.take_ownership(std::move(handle)).encoded_object();
            auto rd = m_parent.m_runtime.remote_descriptor_manager().take_ownership(std::move(handle));

            auto encoded_object = std::make_unique<remote_descriptor::EncodedObject>(rd.encoded_object().proto(),
                                                                                     m_parent.m_runtime.resources());

            // pass T on to the pipeline
            auto ret = m_parent.await_write(std::move(encoded_object));

            return ret;
        }

      private:
        SubscriberParser& m_parent;
    };

    std::shared_ptr<channel::Ingress<memory::TransientBuffer>> channel_ingress() override
    {
        auto parser = std::make_shared<ParserIngress>(*this);

        return parser;
    }

    runtime::Runtime& m_runtime;
};

class InternalSubscriber : public ::srf::pubsub::ISubscriber,
                           public node::SourceChannel<std::unique_ptr<codable::EncodedObject>>
{};

class SubscriberManager : public PubSubBase
{
  public:
    // SubscriberManager(std::string name, runtime::Runtime& runtime) : SubscriberManagerBase(std::move(name), runtime)
    // {}
    SubscriberManager(std::shared_ptr<InternalSubscriber> subscriber) :
      PubSubBase(subscriber, runtime),
      m_subscriber(std::move(subscriber))
    {}

    ~SubscriberManager() override
    {
        Service::call_in_destructor();
    }

    const std::string& role() const final
    {
        return role_subscriber();
    }

    const std::set<std::string>& subscribe_to_roles() const final
    {
        static std::set<std::string> r = {role_publisher()};
        return r;
    }

    // Future<std::shared_ptr<Subscriber<T>>> make_subscriber()
    // {
    //     return m_subscriber_promise.get_future();
    // }

  protected:
    void update_tagged_members(
        ::srf::pubsub::SubscriptionState state,
        const std::unordered_map<std::uint64_t, ::srf::pubsub::SubscriptionMember>& members) final
    {
        bool all_closed = true;

        // todo - convert tagged instances -> tagged endpoints
        for (const auto& [instance_id, member] : members)
        {
            if (member.state != ::srf::pubsub::SubscriptionState::Completed)
            {
                all_closed = false;
            }
        }

        // If all publishers are in a closed state, then detach
        if (all_closed && state == ::srf::pubsub::SubscriptionState::Connected)
        {
            // Call the close function on the subscription
            this->service().close();

            // // This should immediately close all downstream
            // resources().network()->data_plane().server().deserialize_source().drop_edge(this->tag());
        }

        PubSubBase::update_tagged_members(state, members);
    }

  private:
    std::unique_ptr<codable::EncodedObject> handle_network_buffers(memory::TransientBuffer&& buffer)
    {
        // deserialize remote descriptor handle/proto from transient buffer
        auto handle = std::make_unique<srf::codable::protos::RemoteDescriptor>();
        CHECK(handle->ParseFromArray(buffer.data(), buffer.bytes()));

        LOG(INFO) << "transient buffer holding the rd: " << srf::bytes_to_string(buffer.bytes());

        // release transient buffer so it can be reused
        buffer.release();

        // create a remote descriptor via the local RD manager taking ownership of the handle
        std::unique_ptr<codable::EncodedObject> encoded_object =
            runtime().remote_descriptor_manager().take_ownership(std::move(handle)).encoded_object();

        return encoded_object;
    }

    void do_service_start() override
    {
        SubscriptionService::do_service_start();

        CHECK(this->tag() != 0);

        // // Create a parser node (is not a runnable)
        // m_sub_parser = std::make_shared<SubscriberParser>(this->runtime());

        // // Connect the node to our parser
        // node::make_edge(resources().network()->data_plane().server().deserialize_source().source(this->tag()),
        //                 *m_sub_parser);

        // auto launch_options = resources().network()->data_plane().launch_options(1);

        // // Now that the service has started, link the service to the subscriber
        // m_reader = m_subscriber->link_service(this->tag(),
        //                                       this->drop_subscription_service(),
        //                                       resources().runnable().launch_control(),
        //                                       launch_options,
        //                                       *m_sub_parser);

        // auto drop_subscription_service_lambda = drop_subscription_service();

        // auto subscriber = std::shared_ptr<Subscriber<T>>(new Subscriber<T>(service_name(), this->tag()),
        //                                                  [drop_subscription_service_lambda](Subscriber<T>* ptr) {
        //                                                      drop_subscription_service_lambda();
        //                                                      delete ptr;
        //                                                  });

        auto network_reader =
            std::make_unique<node::RxNode<memory::TransientBuffer, std::unique_ptr<codable::EncodedObject>>>(
                rxcpp::operators::map([this](memory::TransientBuffer&& buffer) {
                    return this->handle_network_buffers(std::move(buffer));
                }));

        node::make_edge(resources().network()->data_plane().server().deserialize_source().source(this->tag()),
                        *network_reader);

        // Link the network reader to the ISubscription

        // // Link the subscriber before launching the runnable
        // m_subscriber->link_service(this->tag(), this->drop_subscription_service(), *network_reader);

        auto launch_options = resources().network()->data_plane().launch_options(1);

        this->set_main_runner(resources()
                                  .runnable()
                                  .launch_control()
                                  .prepare_launcher(launch_options, std::move(network_reader))
                                  ->ignition());

        // m_subscriber = subscriber;
        // m_subscriber_promise.set_value(std::move(subscriber));

        SRF_THROW_ON_ERROR(activate_subscription_service());
    }

    // void do_service_await_live() override
    // {
    //     m_reader->await_live();
    // }

    void do_service_stop() override
    {
        // Drop the edge, dont call the base. Let it shut down normally
        resources().network()->data_plane().server().deserialize_source().drop_edge(this->tag());
    }

    void do_service_kill() override
    {
        resources().network()->data_plane().server().deserialize_source().drop_edge(this->tag());

        // Call the base to force kill
        PubSubBase::do_service_kill();
    }

    // void do_service_await_join() override
    // {
    //     m_reader->await_join();
    // }

    // std::shared_ptr<srf::pubsub::SubscriberBase> m_subscriber;
    // std::unique_ptr<srf::runnable::Runner> m_reader;
    // std::shared_ptr<SubscriberParser> m_sub_parser;
    // std::unordered_map<std::uint64_t, InstanceID> m_tagged_instances;
    // Promise<std::shared_ptr<Subscriber<T>>> m_subscriber_promise;
    // srf::node::SourceChannelWriteable<T> m_subcriber_channel;
};

// template <typename T>
// std::shared_ptr<Subscriber<T>> make_subscriber(const std::string& name, runtime::Runtime& runtime)
// {
//     auto manager = std::make_unique<SubscriberManager<T>>(name, runtime);
//     auto future  = manager->make_subscriber();
//     runtime.resources().network()->control_plane().register_subscription_service(std::move(manager));
//     return future.get();
// }

}  // namespace srf::internal::pubsub
