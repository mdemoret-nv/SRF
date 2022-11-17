
#include "srf/pubsub/publisher.hpp"

#include "internal/pubsub/publisher_manager.hpp"
#include "internal/remote_descriptor/encoded_object.hpp"
#include "internal/runtime/runtime.hpp"

#include "srf/core/runtime.hpp"
#include "srf/pubsub/client_subscription_base.hpp"

#include <glog/logging.h>

namespace srf::pubsub {

PublisherBase::PublisherBase(std::string service_name, core::IRuntime& runtime) :
  ClientSubscriptionBase(std::move(service_name), runtime)
{}

const std::string& PublisherBase::role() const
{
    return internal::pubsub::PubSubBase::role_publisher();
}

// const std::string& PublisherBase::service_name() const
// {
//     return m_service_name;
// }

// const std::uint64_t& PublisherBase::tag() const
// {
//     return m_tag;
// }

std::unique_ptr<runnable::Runner> PublisherBase::link_service(
    std::uint64_t tag,
    std::function<void()> drop_service_fn,
    runnable::LaunchControl& launch_control,
    runnable::LaunchOptions& launch_options,
    node::SinkProperties<std::pair<std::uint64_t, std::unique_ptr<srf::remote_descriptor::Storage>>>& data_sink)
{
    // Save the tag
    ClientSubscriptionBase::set_linked_service(tag, std::move(drop_service_fn));

    return this->do_link_service(launch_control, launch_options, data_sink);
}

// void PublisherBase::update_tagged_members(SubscriptionState state,
//                                             const std::unordered_map<std::uint64_t, InstanceID>& tagged_instances)
// {
//     m_tagged_instances = tagged_instances;
//     m_state            = state;

//     this->on_tagged_instances_updated();

//     // Call the on_changed handlers
//     for (auto& change_fn : m_on_connections_changed_fns)
//     {
//         change_fn(m_tagged_instances);
//     }

//     m_tagged_cv.notify_all();
// }

// void PublisherBase::register_connections_changed_handler(connections_changed_handler_t on_changed_fn)
// {
//     m_on_connections_changed_fns.emplace_back(std::move(on_changed_fn));
// }

// const std::unordered_map<std::uint64_t, InstanceID>& PublisherBase::get_tagged_instances() const
// {
//     return m_tagged_instances;
// }

// std::unique_ptr<codable::EncodedObject> PublisherBase::get_encoded_obj() const
// {
//     // Cast our public runtime into the internal runtime
//     auto& runtime = dynamic_cast<internal::runtime::Runtime&>(m_runtime);

//     // Build an internal encoded object and return
//     auto encoded_obj = std::make_unique<internal::remote_descriptor::EncodedObject>(runtime.resources());

//     return encoded_obj;
// }

// void PublisherBase::push_object(std::uint64_t id, std::unique_ptr<remote_descriptor::Storage> storage)
// {
//     LOG(INFO) << "publisher writing object";

//     auto& runtime = dynamic_cast<internal::runtime::Runtime&>(m_runtime);

//     DCHECK(runtime.runnable().main().caller_on_same_thread());

//     auto found = m_tagged_instances.find(id);

//     CHECK(found != m_tagged_instances.end()) << "Tagged ID must be in the list of available instances";

//     internal::data_plane::RemoteDescriptorMessage msg;

//     msg.tag = id;

//     // TODO(MDD): Figure out a better way to get the endpoint
//     msg.endpoint = runtime.resources().network()->data_plane().client().endpoint_shared(found->second);

//     msg.rd = runtime.remote_descriptor_manager().store_object(std::move(storage));
//     CHECK(runtime.resources().network()->data_plane().client().remote_descriptor_channel().await_write(
//               std::move(msg)) == channel::Status::success);
// }

// void PublisherBase::on_tagged_instances_updated()
// {
//     // Do nothing in base
// }

// const std::string& PublisherEdgeBase::service_name()
// {
//     return m_parent.service_name();
// }

// const std::uint64_t& PublisherEdgeBase::tag()
// {
//     return m_parent.tag();
// }

// void PublisherEdgeBase::register_connections_changed_handler(PublisherBase::connections_changed_handler_t
// on_changed_fn)
// {
//     m_parent.register_connections_changed_handler(std::move(on_changed_fn));
// }

// PublisherEdgeBase::PublisherEdgeBase(PublisherBase& parent) : m_parent(parent) {}

// PublisherBase& PublisherEdgeBase::parent() const
// {
//     return m_parent;
// }

// void make_pub_service(std::shared_ptr<PublisherBase> publisher, core::IRuntime& runtime)
// {
//     // Cast the runtime to the internal runtime
//     auto& internal_runtime = dynamic_cast<internal::runtime::Runtime&>(runtime);

//     // Create the new service
//     std::unique_ptr<internal::pubsub::PublisherManager> manager =
//         std::make_unique<internal::pubsub::PublisherManager>(std::move(publisher), internal_runtime);

//     // Start the service
//     internal_runtime.resources().network()->control_plane().register_subscription_service(std::move(manager));

//     // // Capture the drop_service lambda
//     // auto drop_service_fn = manager->get_drop_service_fn();

//     // return drop_service_fn;
// }

std::unique_ptr<IPublisher> make_pub_service(std::string name, core::IRuntime& runtime)
{
    // Cast the runtime to the internal runtime
    auto& internal_runtime = dynamic_cast<internal::runtime::Runtime&>(runtime);

    // Create the new service
    std::unique_ptr<internal::pubsub::PublisherManager> manager =
        std::make_unique<internal::pubsub::PublisherManager>(std::move(name), internal_runtime);

    // Start the service
    internal_runtime.resources().network()->control_plane().register_subscription_service(std::move(manager));
}
}  // namespace srf::pubsub
