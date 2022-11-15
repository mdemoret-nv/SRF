
#include "srf/pubsub/client_subscription_base.hpp"

#include "internal/pubsub/publisher_manager.hpp"
#include "internal/remote_descriptor/encoded_object.hpp"
#include "internal/runtime/runtime.hpp"

#include "srf/core/runtime.hpp"
#include "srf/utils/string_utils.hpp"

#include <glog/logging.h>

namespace srf::pubsub {

ClientSubscriptionBase::ClientSubscriptionBase(std::string service_name, core::IRuntime& runtime) :
  m_service_name(std::move(service_name)),
  m_runtime(runtime)
{
    m_join_future = m_service_completed_promise.get_future().share();
}

ClientSubscriptionBase::~ClientSubscriptionBase()
{
    // Make sure close was called
    this->close();

    // Release any waiters
    m_tagged_cv.notify_all();
}

const std::string& ClientSubscriptionBase::service_name() const
{
    return m_service_name;
}

const std::uint64_t& ClientSubscriptionBase::tag() const
{
    return m_tag;
}

// std::unique_ptr<runnable::Runner> PublisherBase::link_service(
//     std::uint64_t tag,
//     std::function<void()> drop_service_fn,
//     runnable::LaunchControl& launch_control,
//     runnable::LaunchOptions& launch_options,
//     node::SinkProperties<std::pair<std::uint64_t, std::unique_ptr<srf::remote_descriptor::Storage>>>& data_sink)
// {
//     // Save the tag
//     m_tag             = tag;
//     m_drop_service_fn = drop_service_fn;

//     return this->do_link_service(tag, std::move(drop_service_fn), launch_control, launch_options, data_sink);
// }

void ClientSubscriptionBase::register_connections_changed_handler(connections_changed_handler_t on_changed_fn)
{
    m_on_connections_changed_fns.emplace_back(std::move(on_changed_fn));
}

void ClientSubscriptionBase::close()
{
    // Only call this function once
    std::call_once(m_drop_service_fn_called, m_drop_service_fn);
}

void ClientSubscriptionBase::await_join()
{
    m_join_future.get();
}

size_t ClientSubscriptionBase::await_connections()
{
    std::unique_lock lock(m_tagged_mutex);

    m_tagged_cv.wait(lock, [this]() {
        // Wait for instances to be ready
        return !this->get_tagged_instances().empty() || m_state == SubscriptionState::Completed;
    });

    return this->get_tagged_instances().size();
}

void ClientSubscriptionBase::await_completed()
{
    std::unique_lock lock(m_tagged_mutex);

    m_tagged_cv.wait(lock, [this]() {
        // Wait for instances to be ready
        return m_state == SubscriptionState::Completed;
    });
}

void ClientSubscriptionBase::set_linked_service(std::uint64_t tag, std::function<void()> drop_service_fn)
{
    m_tag             = tag;
    m_drop_service_fn = drop_service_fn;
}

const std::unordered_map<std::uint64_t, InstanceID>& ClientSubscriptionBase::get_tagged_instances() const
{
    return m_tagged_instances;
}

std::unique_ptr<codable::EncodedObject> ClientSubscriptionBase::get_encoded_obj() const
{
    // Cast our public runtime into the internal runtime
    auto& runtime = dynamic_cast<internal::runtime::Runtime&>(m_runtime);

    // Build an internal encoded object and return
    auto encoded_obj = std::make_unique<internal::remote_descriptor::EncodedObject>(runtime.resources());

    return encoded_obj;
}

void ClientSubscriptionBase::on_tagged_instances_updated()
{
    // Do nothing in base
}

void ClientSubscriptionBase::update_tagged_instances(
    SubscriptionState state, const std::unordered_map<std::uint64_t, InstanceID>& tagged_instances)
{
    DVLOG(10) << "ClientSubscription: '" << this->service_name() << "/" << this->role()
              << "' updated tagged instances. New State: " << (int)state
              << ", Tags: " << utils::StringUtil::array_to_str(tagged_instances.begin(), tagged_instances.end());

    m_tagged_instances = tagged_instances;
    m_state            = state;

    this->on_tagged_instances_updated();

    // Call the on_changed handlers
    for (auto& change_fn : m_on_connections_changed_fns)
    {
        change_fn(m_tagged_instances);
    }

    m_tagged_cv.notify_all();
}

}  // namespace srf::pubsub
