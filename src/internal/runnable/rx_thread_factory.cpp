#include "srf/runnable/rx_thread_factory.hpp"

#include "internal/system/resources.hpp"
#include "internal/system/system.hpp"
#include "internal/system/system_provider.hpp"
#include "internal/system/thread.hpp"

#include "srf/core/bitmap.hpp"
#include "srf/options/options.hpp"

#include <boost/fiber/future/packaged_task.hpp>

#include <memory>

namespace srf::runnable {

std::thread srf_thread_factory(std::function<void()> task)
{
    auto options = std::make_unique<Options>();
    options->engine_factories().set_default_engine_type(srf::runnable::EngineType::Thread);
    auto srf_system           = srf::internal::system::make_system(std::move(options));
    auto srf_system_provider  = srf::internal::system::SystemProvider(srf_system);
    auto srf_system_resources = srf::internal::system::Resources::create(srf_system_provider);

    boost::fibers::packaged_task<void()> pkg_task(std::move(task));
    auto future = pkg_task.get_future();

    auto srf_thread = std::make_unique<srf::internal::system::Thread>(srf_system_resources->make_thread(
        "srf_thread_factory", options->topology().user_cpuset(), std::move(pkg_task)));

    return srf_thread->release();
}

}  // namespace srf::runnable
