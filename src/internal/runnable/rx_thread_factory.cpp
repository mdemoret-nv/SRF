#include "srf/runnable/rx_thread_factory.hpp"

#include "srf/runnable/thread_context.hpp"

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
    TaskWrapper tw(std::move(task));
    std::packaged_task<void()> pkg_task(std::move(tw));
    return std::thread(std::move(pkg_task));
}

}  // namespace srf::runnable
