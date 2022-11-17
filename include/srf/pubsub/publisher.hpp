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

#include "rxcpp/operators/rx-flat_map.hpp"
#include "rxcpp/operators/rx-map.hpp"
#include "rxcpp/rx-observable.hpp"
#include "rxcpp/rx-subscriber.hpp"
#include "rxcpp/sources/rx-create.hpp"
#include "rxcpp/sources/rx-iterate.hpp"

#include "srf/codable/encoded_object.hpp"
#include "srf/core/runtime.hpp"
#include "srf/node/edge_builder.hpp"
#include "srf/node/generic_sink.hpp"
#include "srf/node/rx_node.hpp"
#include "srf/node/rx_sink.hpp"
#include "srf/node/sink_channel.hpp"
#include "srf/node/sink_properties.hpp"
#include "srf/node/source_channel.hpp"
#include "srf/pubsub/client_subscription_base.hpp"
#include "srf/pubsub/state.hpp"
#include "srf/remote_descriptor/storage.hpp"
#include "srf/runnable/forward.hpp"
#include "srf/runnable/launch_control.hpp"
#include "srf/runnable/launch_options.hpp"
#include "srf/runnable/runnable.hpp"
#include "srf/runnable/runner.hpp"
#include "srf/type_traits.hpp"
#include "srf/types.hpp"
#include "srf/utils/macros.hpp"

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/mutex.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>

namespace srf::internal::pubsub {
class PublisherManager;
}

namespace srf::pubsub {

// TODO(MDD): Use this class when we can have EgressAcceptor nodes
template <typename T>
class EncodeNodeComponent
  : public node::SinkProperties<T>,
    public node::SourceChannel<std::pair<std::uint64_t, std::unique_ptr<srf::remote_descriptor::Storage>>>
{
  public:
    using output_t                = std::pair<std::uint64_t, std::unique_ptr<srf::remote_descriptor::Storage>>;
    using get_id_fn_t             = std::function<std::uint64_t(const T&)>;
    using get_encoded_object_fn_t = std::function<std::unique_ptr<codable::EncodedObject>()>;

    EncodeNodeComponent(get_id_fn_t get_id_fn, get_encoded_object_fn_t get_encoded_object_fn) :
      m_get_id_fn(std::move(get_id_fn)),
      m_get_encoded_object_fn(std::move(get_encoded_object_fn))
    {}

  private:
    struct Upstream : channel::Ingress<T>
    {
        Upstream(EncodeNodeComponent& parent) : m_parent(parent) {}

        ~Upstream() override
        {
            m_parent.release_channel();
        }

        channel::Status await_write(T&& data) override
        {
            // Get the id for this object
            auto id = m_parent.m_get_id_fn(data);

            // Convert to storage
            auto storage =
                remote_descriptor::TypedStorage<T>::create(std::move(data), m_parent.m_get_encoded_object_fn());

            return m_parent.await_write(output_t(id, std::move(storage)));
        }

      private:
        EncodeNodeComponent& m_parent;
    };

    std::shared_ptr<channel::Ingress<T>> channel_ingress() override
    {
        auto parser = std::make_shared<Upstream>(*this);

        return parser;
    }

    get_id_fn_t m_get_id_fn;
    get_encoded_object_fn_t m_get_encoded_object_fn;
};

template <typename T>
class Publisher;

class PublisherBase : public ClientSubscriptionBase
{
  public:
    // using connections_changed_handler_t = std::function<void(const std::unordered_map<std::uint64_t, InstanceID>&)>;

    // const std::string& service_name() const;
    // const std::uint64_t& tag() const;

    // void register_connections_changed_handler(connections_changed_handler_t on_changed_fn);

    // void close()
    // {
    //     // Only call this function once
    //     std::call_once(m_drop_service_fn_called, m_drop_service_fn);
    // }

    // void await_join();

    // size_t await_connections()
    // {
    //     std::unique_lock lock(m_tagged_mutex);

    //     m_tagged_cv.wait(lock, [this]() {
    //         // Wait for instances to be ready
    //         return !this->get_tagged_instances().empty() || m_state == SubscriptionState::Completed;
    //     });

    //     return this->get_tagged_instances().size();
    // }

    // void await_completed()
    // {
    //     std::unique_lock lock(m_tagged_mutex);

    //     m_tagged_cv.wait(lock, [this]() {
    //         // Wait for instances to be ready
    //         return m_state == SubscriptionState::Completed;
    //     });
    // }

    const std::string& role() const final;

  protected:
    PublisherBase(std::string service_name, core::IRuntime& runtime);

    // const std::unordered_map<std::uint64_t, InstanceID>& get_tagged_instances() const;

    // std::unique_ptr<codable::EncodedObject> get_encoded_obj() const;

    // virtual void on_tagged_instances_updated();

  private:
    std::unique_ptr<runnable::Runner> link_service(
        std::uint64_t tag,
        std::function<void()> drop_service_fn,
        runnable::LaunchControl& launch_control,
        runnable::LaunchOptions& launch_options,
        node::SinkProperties<std::pair<std::uint64_t, std::unique_ptr<srf::remote_descriptor::Storage>>>& data_sink);

    virtual std::unique_ptr<runnable::Runner> do_link_service(
        runnable::LaunchControl& launch_control,
        runnable::LaunchOptions& launch_options,
        node::SinkProperties<std::pair<std::uint64_t, std::unique_ptr<srf::remote_descriptor::Storage>>>&
            data_sink) = 0;

    // void update_tagged_members(SubscriptionState state,
    //                              const std::unordered_map<std::uint64_t, InstanceID>& tagged_instances);

    // core::IRuntime& m_runtime;
    // const std::string m_service_name;
    // std::uint64_t m_tag;
    // std::unordered_map<std::uint64_t, InstanceID> m_tagged_instances;

    // std::once_flag m_drop_service_fn_called;
    // std::function<void()> m_drop_service_fn;
    // std::vector<connections_changed_handler_t> m_on_connections_changed_fns;

    // SubscriptionState m_state{SubscriptionState::Watcher};
    // boost::fibers::mutex m_tagged_mutex;
    // boost::fibers::condition_variable m_tagged_cv;

    friend srf::internal::pubsub::PublisherManager;
};

// class PublisherEdgeBase
// {
//   public:
//     DELETE_COPYABILITY(PublisherEdgeBase);
//     DELETE_MOVEABILITY(PublisherEdgeBase);

//     const std::string& service_name();

//     const std::uint64_t& tag();

//     void register_connections_changed_handler(PublisherBase::connections_changed_handler_t on_changed_fn);

//     size_t await_connections()
//     {
//         return m_parent.await_connections();
//     }

//     void await_completed()
//     {
//         m_parent.await_completed();
//     }

//   protected:
//     PublisherEdgeBase(PublisherBase& parent);

//     PublisherBase& parent() const;

//   private:
//     PublisherBase& m_parent;
// };

// template <typename T>
// class PublisherEdge : public node::SinkProperties<T>, private node::SourceChannelWriteable<T>, public
// PublisherEdgeBase
// {
//     PublisherEdge(Publisher<T>& parent) : PublisherEdgeBase(parent) {}

//   public:
//     ~PublisherEdge() = default;

//     using node::SourceChannelWriteable<T>::await_write;

//   private:
//     struct Upstream : channel::Ingress<T>
//     {
//         Upstream(PublisherEdge& parent) : m_parent(parent) {}

//         ~Upstream() override
//         {
//             m_parent.release_channel();
//         }

//         channel::Status await_write(T&& data) override
//         {
//             return m_parent.await_write(std::move(data));
//         }

//       private:
//         PublisherEdge& m_parent;
//     };

//     std::shared_ptr<channel::Ingress<T>> channel_ingress() override
//     {
//         if (is_weak_ptr_null(m_upstream))
//         {
//             // Create and return
//             auto upstream = std::make_shared<Upstream>(*this);

//             m_upstream = upstream;

//             return upstream;
//         }

//         // Been created before, try to lock
//         if (auto upstream = m_upstream.lock())
//         {
//             return upstream;
//         }

//         LOG(FATAL) << "Cannot get channel_ingress. Ingress has already been destroyed.";
//     }

//     std::weak_ptr<Upstream> m_upstream;

//     // Only allow publisher to make this class
//     friend Publisher<T>;
// };

class IPublisher : public IService, public node::SinkChannel<std::unique_ptr<srf::remote_descriptor::Storage>>
{
  public:
    ~IPublisher() override = default;

    virtual std::unique_ptr<codable::EncodedObject> create_storage() = 0;
};

template <typename T>
class Publisher : public node::SinkProperties<T>,
                  private node::SourceChannel<std::unique_ptr<srf::remote_descriptor::Storage>>
{
    // Publisher(std::string service_name, core::IRuntime& runtime) : PublisherBase(std::move(service_name), runtime) {}

    Publisher(std::shared_ptr<IPublisher> publisher) : m_publisher(std::move(publisher))
    {
        // Link the downstream IPublisher
        srf::node::make_edge(*this, *publisher);
    }

  public:
    using data_t   = T;
    using output_t = std::pair<TagID, std::unique_ptr<srf::remote_descriptor::Storage>>;

    ~Publisher() override = default;

    // DELETE_COPYABILITY(Publisher);
    // DELETE_MOVEABILITY(Publisher);

    void close() override
    {
        // // Call the base first
        // PublisherBase::close();
        m_publisher->close();

        // Now drop any connections
        this->release_channel();
    }

  private:
    struct Upstream : channel::Ingress<T>
    {
        Upstream(Publisher& parent) : m_parent(parent) {}

        ~Upstream() override
        {
            // First call close
            m_parent.close();

            // Now release any downstream
            m_parent.release_channel();
        }

        channel::Status await_write(T&& data) override
        {
            auto storage =
                remote_descriptor::TypedStorage<T>::create(std::move(data), m_parent.m_publisher->create_storage());

            return m_parent.await_write(std::move(storage));
        }

      private:
        Publisher& m_parent;
    };

    virtual rxcpp::observable<output_t> build_operators(const rxcpp::observable<T>& start) = 0;

    std::unique_ptr<runnable::Runner> do_link_service(runnable::LaunchControl& launch_control,
                                                      runnable::LaunchOptions& launch_options,
                                                      node::SinkProperties<output_t>& data_sink) override
    {
        // // Now that we have the tag and drop service function, make the edge object
        // auto edge =
        //     std::shared_ptr<PublisherEdge<T>>(new PublisherEdge<T>(*this), [drop_service_fn](PublisherEdge<T>* ptr) {
        //         // Call the function to stop the service
        //         drop_service_fn();
        //         delete ptr;
        //     });

        // m_encode_node =
        //     std::make_shared<EncodeNodeComponent<T>>([this](const T& data) { return this->get_id_for_message(data);
        //     },
        //                                              [this]() { return this->get_encoded_obj(); });

        // Create the sink runnable that will serve as the progress engine
        auto node = std::make_unique<srf::node::RxNode<T, output_t>>(rxcpp::operators::map([](T data) -> output_t {
            // Init with dummy function since we need to delay the operators
            throw std::runtime_error("Should not be hit");
        }));

        node->make_stream([this](const rxcpp::observable<T>& input_obs) {
            return this->build_operators(input_obs.map([this](T data) {
                // Forward to our class
                return this->wait_for_connections(std::move(data));
            }));
        });

        // Link the incoming stream to our node
        srf::node::make_edge(*this, *node);

        // Link our node to the edge
        srf::node::make_edge(*node, data_sink);

        std::unique_ptr<runnable::Runner> writer =
            launch_control.prepare_launcher(launch_options, std::move(node))->ignition();

        // Set the new edge to the edge future
        // m_edge_promise.set_value(std::move(edge));

        return writer;
    }

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
        return nullptr;
    }

    // Future<std::shared_ptr<PublisherEdge<T>>> get_edge()
    // {
    //     // CHECK(m_edge_promise) << "Edge was already used or service was not properly connectd!";

    //     return m_edge_promise.get_future();
    // }

    T wait_for_connections(T&& object)
    {
        // Block until we have tagged instances
        while (this->get_tagged_instances().empty())
        {
            // await subscribers
            // for now just return and drop the object
            boost::this_fiber::yield();
        }

        return object;
    }

    std::weak_ptr<Upstream> m_upstream;
    // Promise<std::shared_ptr<PublisherEdge<T>>> m_edge_promise;
    // std::shared_ptr<EncodeNodeComponent<T>> m_encode_node;

    std::shared_ptr<IPublisher> m_publisher;

    // friend PublisherManager;

    template <typename PublisherT, typename... ArgsT>
    friend auto make_publisher(std::string name, core::IRuntime& runtime, ArgsT&&... args);
};

template <typename T>
class PublisherRoundRobin : public Publisher<T>
{
  public:
    using Publisher<T>::Publisher;
    using typename Publisher<T>::output_t;

  private:
    rxcpp::observable<output_t> build_operators(const rxcpp::observable<T>& start) override
    {
        return start | rxcpp::operators::map([this](T object) {
                   auto storage =
                       remote_descriptor::TypedStorage<T>::create(std::move(object), this->get_encoded_obj());

                   auto next_id = m_tagged_ids[m_next_idx++ % m_tagged_ids.size()];

                   return output_t(next_id, std::move(storage));
               });
    }

    void on_tagged_instances_updated() final
    {
        // Save the IDs in a list for quick lookup
        m_tagged_ids.clear();

        for (auto const& id : this->get_tagged_instances())
        {
            m_tagged_ids.push_back(id.first);
        }
        m_next_idx = 0;

        // Make sure to call the base
        Publisher<T>::on_tagged_instances_updated();
    }

    // std::uint64_t get_id_for_message(const T& data) final
    // {
    //     auto next_id = m_tagged_ids[m_next_idx++ % m_tagged_ids.size()];

    //     return next_id;
    // }

    // output_t write(T&& object) final
    // {
    //     auto storage = remote_descriptor::TypedStorage<T>::create(std::move(object), this->get_encoded_obj());

    //     auto next_id = m_tagged_ids[m_next_idx++ % m_tagged_ids.size()];

    //     return output_t(next_id, std::move(storage));
    // }

    std::atomic<std::size_t> m_next_idx{0};
    std::vector<std::uint64_t> m_tagged_ids;
};

// template <typename T>
// class PublisherBroadcast : public Publisher<T>
// {
//   public:
//     using Publisher<T>::Publisher;
//     using typename Publisher<T>::output_t;

//   private:
//     rxcpp::observable<output_t> build_operators(const rxcpp::observable<T>& start) override
//     {
//         return start.flat_map([this](T object) {
//             return rxcpp::sources::create<output_t>(
//                 [this, object = std::move(object)](rxcpp::subscriber<output_t> s) mutable {
//                     std::vector<std::shared_ptr<codable::EncodedObject>> encoded_objects(m_tagged_ids.size(),
//                     nullptr);

//                     // Build the encoded object list
//                     std::generate(
//                         encoded_objects.begin(), encoded_objects.end(), [this]() { return this->get_encoded_obj();
//                         });

//                     // auto storages =
//                     //     remote_descriptor::TypedStorage<T>::create_many(std::move(object),
//                     //     std::move(encoded_objects));

//                     std::vector<std::unique_ptr<remote_descriptor::TypedStorage<T>>> storages;

//                     std::vector<output_t> outputs;

//                     for (size_t i = 0; i < storages.size(); ++i)
//                     {
//                         s.on_next(output_t(m_tagged_ids[i], std::move(storages[i])));
//                     }

//                     // std::transform(m_tagged_ids.begin(),
//                     //                m_tagged_ids.end(),
//                     //                storages.begin(),
//                     //                std::back_inserter(outputs),
//                     //                [](const auto& next_id, auto storage) { return output_t(next_id,
//                     //                std::move(storage)); });

//                     // for (auto& x : outputs)
//                     // {
//                     //     s.on_next(std::move(x));
//                     // }

//                     s.on_completed();
//                 });
//         });
//     }

//     void on_tagged_instances_updated() final
//     {
//         // Save the IDs in a list for quick lookup
//         m_tagged_ids.clear();

//         for (auto const& id : this->get_tagged_instances())
//         {
//             m_tagged_ids.push_back(id.first);
//         }

//         // Make sure to call the base
//         Publisher<T>::on_tagged_instances_updated();
//     }

//     std::vector<TagID> m_tagged_ids;
// };

std::unique_ptr<IPublisher> make_pub_service(std::string name, core::IRuntime& runtime);

template <typename T>
auto make_publisher(std::string name, core::IRuntime& runtime)
{
    // Create the internal publisher object
    auto internal_publisher = make_pub_service(std::move(name), runtime);

    // Get the data type
    // using data_t = typename PublisherT::data_t;

    // // Create the edge object
    // auto pub_edge = std::make_shared<PublisherEdge<data_t>>();

    // Create the actual publisher
    auto pub = std::shared_ptr<Publisher<T>>(new Publisher<T>(std::move(internal_publisher)));

    // Get a future to the edge that will be created during make_pub_service
    // auto pub_edge_future = pub->get_edge();

    // // Now build the service
    // make_pub_service(pub, runtime);

    // // Now set the drop function into the publisher edge
    // pub_edge->m_drop_service_fn = std::move(drop_service_fn);

    // Finally, return the edge
    return pub;
}

// void make_pub_service(std::shared_ptr<PublisherBase> publisher, core::IRuntime& runtime);

}  // namespace srf::pubsub
