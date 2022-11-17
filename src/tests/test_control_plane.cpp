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

#include "common.hpp"

#include "internal/control_plane/client.hpp"
#include "internal/control_plane/client/connections_manager.hpp"
#include "internal/control_plane/resources.hpp"
#include "internal/control_plane/server.hpp"
#include "internal/data_plane/resources.hpp"
#include "internal/grpc/client_streaming.hpp"
#include "internal/grpc/server.hpp"
#include "internal/grpc/server_streaming.hpp"
#include "internal/network/resources.hpp"
#include "internal/pubsub/publisher_manager.hpp"
#include "internal/pubsub/subscriber_manager.hpp"
#include "internal/resources/manager.hpp"
#include "internal/runtime/runtime.hpp"
#include "internal/service.hpp"

#include "srf/codable/fundamental_types.hpp"  // IWYU pragma: keep
#include "srf/memory/buffer.hpp"
#include "srf/memory/codable/buffer.hpp"
#include "srf/memory/literals.hpp"
#include "srf/node/edge_builder.hpp"
#include "srf/node/rx_sink.hpp"
#include "srf/node/sink_channel.hpp"
#include "srf/node/sink_properties.hpp"
#include "srf/node/source_channel.hpp"
#include "srf/options/placement.hpp"
#include "srf/protos/architect.grpc.pb.h"
#include "srf/protos/architect.pb.h"
#include "srf/pubsub/publisher.hpp"
#include "srf/pubsub/subscriber.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <optional>
#include <set>
#include <stack>
#include <thread>

using namespace srf;
using namespace srf::memory::literals;

static auto make_runtime(std::function<void(Options& options)> options_lambda = [](Options& options) {})
{
    auto resources = std::make_unique<internal::resources::Manager>(
        internal::system::SystemProvider(make_system([&](Options& options) {
            options.topology().user_cpuset("0-3");
            options.topology().restrict_gpus(true);
            options.placement().resources_strategy(PlacementResources::Dedicated);
            options.placement().cpu_strategy(PlacementStrategy::PerMachine);
            options_lambda(options);
        })));

    return std::make_unique<internal::runtime::RuntimeManager>(std::move(resources));
}

class TestControlPlane : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        // Create a runtime for the server
        m_server_runtime = make_runtime();

        // Create the server object
        m_server = std::make_unique<internal::control_plane::Server>(
            m_server_runtime->runtime(0).resources().runnable(), std::chrono::milliseconds(10));

        this->start_server();
    }

    void TearDown() override
    {
        // Delete any remaining runtimes. Must happen before closing the server
        while (!m_runtimes.empty())
        {
            m_runtimes.pop();
        }

        // Tear down the server
        this->stop_server();

        m_server.reset();

        // Finally, close the server runtime. Must be done after the server
        m_server_runtime.reset();
    }

    void start_server()
    {
        m_server->service_start();
        m_server->service_await_live();
    }

    void stop_server()
    {
        if (m_server->state() == internal::ServiceState::Running)
        {
            m_server->service_stop();
            m_server->service_await_join();
        }
    }

    internal::runtime::RuntimeManager& make_client_runtime(std::string user_cpuset)
    {
        return *m_runtimes.emplace(make_runtime([user_cpuset](Options& options) {
            options.topology().user_cpuset(user_cpuset);
            options.topology().restrict_gpus(true);
            options.architect_url("localhost:13337");
        }));
    }

    std::unique_ptr<internal::runtime::RuntimeManager> m_server_runtime;
    std::unique_ptr<internal::control_plane::Server> m_server;
    std::stack<std::unique_ptr<internal::runtime::RuntimeManager>> m_runtimes;
};

TEST_F(TestControlPlane, LifeCycle)
{
    // Do nothing in here. Entire tests is conducted in SetUp()/TearDown()

    // auto sr     = make_runtime();
    // auto server = std::make_unique<internal::control_plane::Server>(sr->runtime(0).resources().runnable());

    // server->service_start();
    // server->service_await_live();

    // // inspect server

    // server->service_stop();
    // server->service_await_join();
}

TEST_F(TestControlPlane, SingleClientConnectDisconnect)
{
    auto cr = make_runtime([](Options& options) { options.architect_url("localhost:13337"); });

    // the total number of partition is system dependent
    auto expected_partitions = cr->resources().system().partitions().flattened().size();
    EXPECT_EQ(cr->runtime(0).resources().network()->control_plane().client().connections().instance_ids().size(),
              expected_partitions);

    // destroying the resources should gracefully shutdown the data plane and the control plane.
    cr.reset();
}

TEST_F(TestControlPlane, DoubleClientConnectExchangeDisconnect)
{
    auto client_1 = make_runtime([](Options& options) {
        options.topology().user_cpuset("0-3");
        options.topology().restrict_gpus(true);
        options.architect_url("localhost:13337");
    });

    auto client_2 = make_runtime([](Options& options) {
        options.topology().user_cpuset("4-7");
        options.topology().restrict_gpus(true);
        options.architect_url("localhost:13337");
    });

    // the total number of partition is system dependent
    auto expected_partitions_1 = client_1->resources().system().partitions().flattened().size();
    EXPECT_EQ(client_1->runtime(0).resources().network()->control_plane().client().connections().instance_ids().size(),
              expected_partitions_1);

    auto expected_partitions_2 = client_2->resources().system().partitions().flattened().size();
    EXPECT_EQ(client_2->runtime(0).resources().network()->control_plane().client().connections().instance_ids().size(),
              expected_partitions_2);

    auto f1 = client_1->runtime(0).resources().network()->control_plane().client().connections().update_future();
    auto f2 = client_2->runtime(0).resources().network()->control_plane().client().connections().update_future();

    client_1->runtime(0).resources().network()->control_plane().client().request_update();

    f1.get();
    f2.get();

    client_1->runtime(0)
        .resources()
        .runnable()
        .main()
        .enqueue([&] {
            auto worker_count = client_1->runtime(0)
                                    .resources()
                                    .network()
                                    ->control_plane()
                                    .client()
                                    .connections()
                                    .worker_addresses()
                                    .size();
            EXPECT_EQ(worker_count, expected_partitions_1 + expected_partitions_2);
        })
        .get();

    // destroying the resources should gracefully shutdown the data plane and the control plane.
    client_1.reset();
    client_2.reset();
}

TEST_F(TestControlPlane, PubCreatedFirst)
{
    auto& client_1 = make_client_runtime("0");
    auto& client_2 = make_client_runtime("1");

    auto publisher = srf::pubsub::make_publisher<pubsub::PublisherRoundRobin<int>>("my_int", client_1.runtime(0));

    auto subscriber = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_2.runtime(0));

    // Wait for the subscriber and publisher to have connections
    EXPECT_EQ(publisher->await_connections(), 1);
    EXPECT_EQ(subscriber->await_connections(), 1);

    publisher->close();
    subscriber->close();

    // Publisher has been deleted, subscriber should be closed. Await the service
    publisher->await_join();
    subscriber->await_join();

    auto service1 = client_1.runtime(0).resources().network()->control_plane().get_subscription_service("my_int");
    EXPECT_EQ(service1.size(), 0);
    auto service2 = client_1.runtime(0).resources().network()->control_plane().get_subscription_service("my_int");
    EXPECT_EQ(service2.size(), 0);
}

TEST_F(TestControlPlane, SubCreatedFirst)
{
    auto& client_1 = make_client_runtime("0");
    auto& client_2 = make_client_runtime("1");

    auto subscriber = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_2.runtime(0));

    auto publisher = srf::pubsub::make_publisher<pubsub::PublisherRoundRobin<int>>("my_int", client_1.runtime(0));

    // Wait for the subscriber and publisher to have connections
    EXPECT_EQ(subscriber->await_connections(), 1);
    EXPECT_EQ(publisher->await_connections(), 1);

    subscriber->close();
    publisher->close();

    // Publisher has been deleted, subscriber should be closed. Await the service
    subscriber->await_join();
    publisher->await_join();

    auto service1 = client_1.runtime(0).resources().network()->control_plane().get_subscription_service("my_int");
    EXPECT_EQ(service1.size(), 0);
    auto service2 = client_1.runtime(0).resources().network()->control_plane().get_subscription_service("my_int");
    EXPECT_EQ(service2.size(), 0);
}

TEST_F(TestControlPlane, PubCloseByEdge)
{
    auto& client_1 = make_client_runtime("0");

    auto publisher = srf::pubsub::make_publisher<pubsub::PublisherRoundRobin<int>>("my_int", client_1.runtime(0));

    // Create an edge to the publisher
    auto source = std::make_shared<node::SourceChannelWriteable<int>>();

    srf::node::make_edge(*source, *publisher);

    // Now delete the edge which should close the publisher
    source.reset();

    // Publisher should close on its own
    publisher->await_join();
}

TEST_F(TestControlPlane, SubCloseByEdge)
{
    auto& client_1 = make_client_runtime("0");

    auto subscriber = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_1.runtime(0));

    // Create an edge to the subscriber
    auto sink = std::make_shared<node::SinkChannelReadable<int>>();

    srf::node::make_edge(*subscriber, *sink);

    // Now delete the edge which should close the publisher
    sink.reset();

    // Subscriber should close on its own
    subscriber->await_join();
}

TEST_F(TestControlPlane, PubSubShutdown)
{
    auto& client_1 = make_client_runtime("0");
    auto& client_2 = make_client_runtime("1");

    auto publisher = srf::pubsub::make_publisher<pubsub::PublisherRoundRobin<int>>("my_int", client_1.runtime(0));

    auto subscriber = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_2.runtime(0));

    // publisher->await_connections();

    // client_1->runtime(0).resources().network()->control_plane().client().request_update();

    // Wait for the subscriber and publisher to have connections
    EXPECT_EQ(subscriber->await_connections(), 1);
    EXPECT_EQ(publisher->await_connections(), 1);

    // LOG(INFO) << "AFTER SLEEP 1 - publisher should have 1 subscriber";
    // client-side: publisher manager should have 1 tagged instance in it write list
    // server-side: publisher member list: 1, subscriber member list: 1, subscriber subscribe_to list: 1

    LOG(INFO) << "[START] CLOSE PUBLISHER";
    publisher->close();
    LOG(INFO) << "[FINISH] CLOSE PUBLISHER";

    // Wait for the publisher to be closed
    publisher->await_join();

    LOG(INFO) << "PUBLISHER JOIN COMPLETED";

    // client_1->runtime(0).resources().network()->control_plane().client().request_update();

    // Publisher has been closed by subscriber hasnt. This should end eventually
    subscriber->await_join();

    LOG(INFO) << "SUBSCRIBER JOIN COMPLETED";

    // Make sure we can call close() after the fact without breaking anything
    subscriber->close();

    // Try to find the service
    auto services = client_1.runtime(0).resources().network()->control_plane().get_subscription_service("my_int");

    // They should all be deleted
    EXPECT_EQ(services.size(), 0);
}

TEST_F(TestControlPlane, DoubleClientPubSub)
{
    auto& client_1 = make_client_runtime("0");
    auto& client_2 = make_client_runtime("1");

    // the total number of partition is system dependent
    auto expected_partitions_1 = client_1.resources().system().partitions().flattened().size();
    EXPECT_EQ(client_1.runtime(0).resources().network()->control_plane().client().connections().instance_ids().size(),
              expected_partitions_1);

    auto expected_partitions_2 = client_2.resources().system().partitions().flattened().size();
    EXPECT_EQ(client_2.runtime(0).resources().network()->control_plane().client().connections().instance_ids().size(),
              expected_partitions_2);

    auto f1 = client_1.runtime(0).resources().network()->control_plane().client().connections().update_future();
    auto f2 = client_2.runtime(0).resources().network()->control_plane().client().connections().update_future();

    client_1.runtime(0).resources().network()->control_plane().client().request_update();

    LOG(INFO) << "Update futures - waiting";

    f1.get();
    f2.get();

    LOG(INFO) << "Update futures - finished";

    client_1.runtime(0)
        .resources()
        .runnable()
        .main()
        .enqueue([&] {
            auto worker_count = client_1.runtime(0)
                                    .resources()
                                    .network()
                                    ->control_plane()
                                    .client()
                                    .connections()
                                    .worker_addresses()
                                    .size();
            EXPECT_EQ(worker_count, expected_partitions_1 + expected_partitions_2);
        })
        .get();

    LOG(INFO) << "MAKE PUBLISHER";

    // auto publisher = internal::pubsub::make_publisher<int>(
    //     "my_int", internal::pubsub::PublisherType::RoundRobin, client_1->runtime(0));

    auto publisher = srf::pubsub::make_publisher<pubsub::PublisherRoundRobin<int>>("my_int", client_1.runtime(0));

    auto subscriber = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_2.runtime(0));

    auto source = std::make_shared<node::SourceChannelWriteable<int>>();

    srf::node::make_edge(*source, *publisher);

    auto sink = std::make_shared<node::SinkChannelReadable<int>>();

    srf::node::make_edge(*subscriber, *sink);

    source->await_write(42);
    source->await_write(15);

    int output = 0;
    sink->egress().await_read(output);
    EXPECT_EQ(output, 42);

    sink->egress().await_read(output);
    EXPECT_EQ(output, 15);

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    LOG(INFO) << "AFTER SLEEP 1 - publisher should have 1 subscriber";
    // client-side: publisher manager should have 1 tagged instance in it write list
    // server-side: publisher member list: 1, subscriber member list: 1, subscriber subscribe_to list: 1

    LOG(INFO) << "[START] DELETE SUBSCRIBER";
    subscriber->close();
    LOG(INFO) << "[FINISH] DELETE SUBSCRIBER";

    client_1.runtime(0).resources().network()->control_plane().client().request_update();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    LOG(INFO) << "AFTER SLEEP 2 - publisher should have 0 subscribers";

    LOG(INFO) << "[START] DELETE PUBLISHER";
    publisher->close();
    source.reset();
    LOG(INFO) << "[FINISH] DELETE PUBLISHER";

    client_1.runtime(0).resources().network()->control_plane().client().request_update();

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    subscriber->await_join();
    publisher->await_join();
}

TEST_F(TestControlPlane, SinglePubMultiSub)
{
    auto& client_1 = make_client_runtime("0");
    auto& client_2 = make_client_runtime("1");
    auto& client_3 = make_client_runtime("2");

    auto subscriber1 = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_2.runtime(0));
    auto subscriber2 = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_3.runtime(0));

    auto publisher = srf::pubsub::make_publisher<pubsub::PublisherRoundRobin<int>>("my_int", client_1.runtime(0));

    // Wait for the subscriber and publisher to have connections
    EXPECT_EQ(subscriber1->await_connections(), 1);
    EXPECT_EQ(subscriber2->await_connections(), 1);
    EXPECT_EQ(publisher->await_connections(), 2);

    auto source = std::make_shared<node::SourceChannelWriteable<int>>();

    srf::node::make_edge(*source, *publisher);

    auto sink1 = std::make_shared<node::SinkChannelReadable<int>>();
    auto sink2 = std::make_shared<node::SinkChannelReadable<int>>();

    srf::node::make_edge(*subscriber1, *sink1);
    srf::node::make_edge(*subscriber2, *sink2);

    source->await_write(42);
    source->await_write(15);

    int output = 0;
    sink1->egress().await_read(output);
    EXPECT_EQ(output, 42);

    sink2->egress().await_read(output);
    EXPECT_EQ(output, 15);

    subscriber1->close();
    subscriber2->close();
    publisher->close();

    // Publisher has been deleted, subscriber should be closed. Await the service
    subscriber1->await_join();
    subscriber2->await_join();
    publisher->await_join();

    // auto service = client_1.runtime(0).resources().network()->control_plane().get_subscription_service("my_int")[0];

    // EXPECT_EQ(service.get().state(), internal::ServiceState::Completed);
}

// TEST_F(TestControlPlane, SinglePubMultiSubBroadcast)
// {
//     auto& client_1 = make_client_runtime("0");
//     auto& client_2 = make_client_runtime("1");
//     auto& client_3 = make_client_runtime("2");

//     auto subscriber1 = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_2.runtime(0));
//     auto subscriber2 = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_3.runtime(0));

//     auto publisher = srf::pubsub::make_publisher<pubsub::PublisherBroadcast<int>>("my_int", client_1.runtime(0));

//     // Wait for the subscriber and publisher to have connections
//     EXPECT_EQ(subscriber1->await_connections(), 1);
//     EXPECT_EQ(subscriber2->await_connections(), 1);
//     EXPECT_EQ(publisher->await_connections(), 2);

//     auto source = std::make_shared<node::SourceChannelWriteable<int>>();

//     srf::node::make_edge(*source, *publisher);

//     auto sink1 = std::make_shared<node::SinkChannelReadable<int>>();
//     auto sink2 = std::make_shared<node::SinkChannelReadable<int>>();

//     srf::node::make_edge(*subscriber1, *sink1);
//     srf::node::make_edge(*subscriber2, *sink2);

//     source->await_write(42);
//     source->await_write(15);

//     int output = 0;
//     sink1->egress().await_read(output);
//     EXPECT_EQ(output, 42);

//     sink2->egress().await_read(output);
//     EXPECT_EQ(output, 15);

//     subscriber1->close();
//     subscriber2->close();
//     publisher->close();

//     // Publisher has been deleted, subscriber should be closed. Await the service
//     subscriber1->await_join();
//     subscriber2->await_join();
//     publisher->await_join();

//     // auto service =
//     client_1.runtime(0).resources().network()->control_plane().get_subscription_service("my_int")[0];

//     // EXPECT_EQ(service.get().state(), internal::ServiceState::Completed);
// }

TEST_F(TestControlPlane, SingleSubMultiPub)
{
    auto& client_1 = make_client_runtime("0");
    auto& client_2 = make_client_runtime("1");
    auto& client_3 = make_client_runtime("2");

    auto publisher1 = srf::pubsub::make_publisher<pubsub::PublisherRoundRobin<int>>("my_int", client_1.runtime(0));
    auto publisher2 = srf::pubsub::make_publisher<pubsub::PublisherRoundRobin<int>>("my_int", client_2.runtime(0));

    auto subscriber = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_int", client_3.runtime(0));

    // Wait for the subscriber and publisher to have connections
    EXPECT_EQ(publisher1->await_connections(), 1);
    EXPECT_EQ(publisher2->await_connections(), 1);
    EXPECT_EQ(subscriber->await_connections(), 2);

    auto source1 = std::make_shared<node::SourceChannelWriteable<int>>();
    auto source2 = std::make_shared<node::SourceChannelWriteable<int>>();

    srf::node::make_edge(*source1, *publisher1);
    srf::node::make_edge(*source2, *publisher2);

    auto sink = std::make_shared<node::SinkChannelReadable<int>>();

    srf::node::make_edge(*subscriber, *sink);

    source1->await_write(42);
    source2->await_write(15);

    int output = 0;
    sink->egress().await_read(output);
    EXPECT_EQ(output, 42);

    sink->egress().await_read(output);
    EXPECT_EQ(output, 15);

    subscriber->close();
    publisher1->close();
    publisher2->close();

    // Publisher has been deleted, subscriber should be closed. Await the service
    subscriber->await_join();
    publisher1->await_join();
    publisher2->await_join();

    // auto service = client_1.runtime(0).resources().network()->control_plane().get_subscription_service("my_int")[0];

    // EXPECT_EQ(service.get().state(), internal::ServiceState::Completed);
}

TEST_F(TestControlPlane, DoubleClientPubSubBuffers)
{
    // auto sr     = make_runtime();
    // auto server = std::make_unique<internal::control_plane::Server>(sr->runtime(0).resources().runnable());

    // server->service_start();
    // server->service_await_live();

    auto client_1 = make_runtime([](Options& options) {
        options.topology().user_cpuset("0-3");
        options.topology().restrict_gpus(true);
        options.architect_url("localhost:13337");
        options.resources().enable_device_memory_pool(false);
        options.resources().enable_host_memory_pool(true);
        options.resources().host_memory_pool().block_size(32_MiB);
        options.resources().host_memory_pool().max_aggregate_bytes(128_MiB);
        options.resources().device_memory_pool().block_size(64_MiB);
        options.resources().device_memory_pool().max_aggregate_bytes(128_MiB);
    });

    auto client_2 = make_runtime([](Options& options) {
        options.topology().user_cpuset("4-7");
        options.topology().restrict_gpus(true);
        options.architect_url("localhost:13337");
    });

    // the total number of partition is system dependent
    auto expected_partitions_1 = client_1->resources().system().partitions().flattened().size();
    EXPECT_EQ(client_1->runtime(0).resources().network()->control_plane().client().connections().instance_ids().size(),
              expected_partitions_1);

    auto expected_partitions_2 = client_2->resources().system().partitions().flattened().size();
    EXPECT_EQ(client_2->runtime(0).resources().network()->control_plane().client().connections().instance_ids().size(),
              expected_partitions_2);

    auto f1 = client_1->runtime(0).resources().network()->control_plane().client().connections().update_future();
    auto f2 = client_2->runtime(0).resources().network()->control_plane().client().connections().update_future();

    client_1->runtime(0).resources().network()->control_plane().client().request_update();

    f1.get();
    f2.get();

    client_1->runtime(0)
        .resources()
        .runnable()
        .main()
        .enqueue([&] {
            auto worker_count = client_1->runtime(0)
                                    .resources()
                                    .network()
                                    ->control_plane()
                                    .client()
                                    .connections()
                                    .worker_addresses()
                                    .size();
            EXPECT_EQ(worker_count, expected_partitions_1 + expected_partitions_2);
        })
        .get();

    LOG(INFO) << "MAKE PUBLISHER";

    auto publisher = srf::pubsub::make_publisher<pubsub::PublisherRoundRobin<int>>("my_buffer", client_1->runtime(0));

    LOG(INFO) << "MAKE SUBSCRIBER";
    auto subscriber = srf::pubsub::make_subscriber<pubsub::Subscriber<int>>("my_buffer", client_2->runtime(0));

    client_1->runtime(0).resources().network()->control_plane().client().request_update();

    auto buffer_1 = client_1->runtime(0).resources().host().make_buffer(4_MiB);
    auto buffer_2 = client_1->runtime(0).resources().host().make_buffer(4_MiB);

    auto source = node::SourceChannelWriteable<int>();

    srf::node::make_edge(source, *publisher);

    source.await_write(std::move(buffer_1));
    source.await_write(std::move(buffer_2));

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    LOG(INFO) << "AFTER SLEEP 1 - publisher should have 1 subscriber";
    // client-side: publisher manager should have 1 tagged instance in it write list
    // server-side: publisher member list: 1, subscriber member list: 1, subscriber subscribe_to list: 1

    LOG(INFO) << "[START] DELETE SUBSCRIBER";
    subscriber.reset();
    LOG(INFO) << "[FINISH] DELETE SUBSCRIBER";

    client_1->runtime(0).resources().network()->control_plane().client().request_update();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    LOG(INFO) << "AFTER SLEEP 2 - publisher should have 0 subscribers";

    LOG(INFO) << "[START] DELETE PUBLISHER";
    publisher.reset();
    LOG(INFO) << "[FINISH] DELETE PUBLISHER";

    client_1->runtime(0).resources().network()->control_plane().client().request_update();

    // destroying the resources should gracefully shutdown the data plane and the control plane.
    client_1.reset();
    client_2.reset();

    // server->service_stop();
    // server->service_await_join();
}
