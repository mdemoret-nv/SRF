#include "srf/runnable/rx_thread_factory.hpp"

#include "internal/system/resources.hpp"
#include "internal/system/system.hpp"
#include "internal/system/system_provider.hpp"
#include "internal/system/thread.hpp"

#include "srf/core/bitmap.hpp"
#include "srf/options/options.hpp"
#include "srf/runnable/thread_context.hpp"

#include <boost/fiber/future/packaged_task.hpp>

#include <functional>
#include <memory>
#include <thread>

namespace srf::runnable {

class TaskWrapper
{
  public:
    TaskWrapper(std::function<void()> task) : m_task{std::move(task)} {};
    void operator()()
    {
        auto tcr = std::make_shared<ThreadContextResources>(1);
        ThreadContext tc(tcr, 1, 1);
        tc.init_tmp();
        auto& context = srf::runnable::Context::get_runtime_context();
        VLOG(1) << "ThreadContext: " << context.info();
        m_task();
    }

  private:
    std::function<void()> m_task;
};

std::thread srf_thread_factory(std::function<void()> task)
{
    auto options = std::make_unique<Options>();
    options->engine_factories().set_default_engine_type(srf::runnable::EngineType::Thread);

    auto srf_system           = srf::internal::system::make_system(std::move(options));
    auto srf_system_provider  = srf::internal::system::SystemProvider(srf_system);
    auto srf_system_resources = srf::internal::system::Resources::create(srf_system_provider);

    TaskWrapper tw(std::move(task));
    std::packaged_task<void()> pkg_task(std::move(tw));
    std::future<void> future = pkg_task.get_future();

    auto srf_thread = std::make_unique<srf::internal::system::Thread>(
        srf_system_resources->make_thread("srf_thread_factory", srf_system->topology().cpu_set(), pkg_task));

    std::thread thread = srf_thread->release();

    return thread;
}

}  // namespace srf::runnable
