#pragma once

#include "srf/core/fiber_pool.hpp"
#include "srf/runnable/context.hpp"
#include "srf/runnable/engine.hpp"
#include "srf/types.hpp"

#include <boost/fiber/operations.hpp>
#include <glog/logging.h>
#include <rxcpp/operators/rx-observe_on.hpp>

#include <atomic>
#include <functional>
#include <thread>

namespace srf::runnable {

struct srf_scheduler : public rxcpp::schedulers::scheduler_interface
{
  private:
    using this_type = srf_scheduler;
    srf_scheduler(const this_type&);

    struct new_worker : public rxcpp::schedulers::worker_interface
    {
      private:
        using this_type = new_worker;

        using queue_type = rxcpp::schedulers::detail::action_queue;

        new_worker(const this_type&);

        struct new_worker_state : public std::enable_shared_from_this<new_worker_state>
        {
            using queue_item_time = rxcpp::schedulers::detail::schedulable_queue<typename clock_type::time_point>;

            using item_type = queue_item_time::item_type;

            virtual ~new_worker_state() {}

            explicit new_worker_state(rxcpp::composite_subscription cs) : lifetime(cs) {}

            rxcpp::composite_subscription lifetime;
            mutable std::mutex lock;
            mutable std::condition_variable wake;
            mutable queue_item_time q;
            // std::thread worker;
            Future<void> worker_future;
            rxcpp::schedulers::recursion r;
        };

        std::shared_ptr<new_worker_state> state;

      public:
        virtual ~new_worker() {}

        explicit new_worker(std::shared_ptr<new_worker_state> ws) : state(ws) {}

        new_worker(rxcpp::composite_subscription cs, rxcpp::schedulers::thread_factory& tf) :
          state(std::make_shared<new_worker_state>(cs))
        {
            auto keepAlive = state;

            state->lifetime.add([keepAlive]() {
                std::unique_lock<std::mutex> guard(keepAlive->lock);
                auto expired = std::move(keepAlive->q);
                keepAlive->q = new_worker_state::queue_item_time{};
                if (!keepAlive->q.empty())
                    std::terminate();
                keepAlive->wake.notify_one();

                if (keepAlive->worker_future.valid())
                {
                    guard.unlock();
                    keepAlive->worker_future.wait();
                }
                else
                {
                    // keepAlive->worker.detach();
                }
            });

            auto& ctx = runnable::Context::get_runtime_context();

            auto fut = ctx.engine()->run_task([keepAlive]() {
                // Debug message
                VLOG(10) << "Running test task";

                // take ownership
                queue_type::ensure(std::make_shared<new_worker>(keepAlive));
            });

            fut.wait();

            state->worker_future = ctx.engine()->run_task([keepAlive]() {
                // take ownership
                queue_type::ensure(std::make_shared<new_worker>(keepAlive));
                // release ownership
                // RXCPP_UNWIND_AUTO([] { queue_type::destroy(); });

                for (;;)
                {
                    std::unique_lock<std::mutex> guard(keepAlive->lock);
                    if (keepAlive->q.empty())
                    {
                        keepAlive->wake.wait(guard, [keepAlive]() {
                            return !keepAlive->lifetime.is_subscribed() || !keepAlive->q.empty();
                        });
                    }
                    if (!keepAlive->lifetime.is_subscribed())
                    {
                        break;
                    }
                    auto& peek = keepAlive->q.top();
                    if (!peek.what.is_subscribed())
                    {
                        keepAlive->q.pop();
                        continue;
                    }
                    auto when = peek.when;
                    if (clock_type::now() < when)
                    {
                        keepAlive->wake.wait_until(guard, when);
                        continue;
                    }
                    auto what = peek.what;
                    keepAlive->q.pop();
                    keepAlive->r.reset(keepAlive->q.empty());
                    guard.unlock();
                    what(keepAlive->r.get_recurse());
                }
            });

            ctx.yield();

            state->worker_future.wait();
        }

        virtual clock_type::time_point now() const
        {
            return clock_type::now();
        }

        virtual void schedule(const rxcpp::schedulers::schedulable& scbl) const
        {
            schedule(now(), scbl);
        }

        virtual void schedule(clock_type::time_point when, const rxcpp::schedulers::schedulable& scbl) const
        {
            if (scbl.is_subscribed())
            {
                std::unique_lock<std::mutex> guard(state->lock);
                state->q.push(new_worker_state::item_type(when, scbl));
                state->r.reset(false);
            }
            state->wake.notify_one();
        }
    };

    mutable rxcpp::schedulers::thread_factory factory;

  public:
    srf_scheduler() {}
    explicit srf_scheduler(rxcpp::schedulers::thread_factory tf) : factory(tf) {}
    virtual ~srf_scheduler() {}

    virtual clock_type::time_point now() const
    {
        return clock_type::now();
    }

    virtual rxcpp::schedulers::worker create_worker(rxcpp::composite_subscription cs) const
    {
        return {cs, std::make_shared<new_worker>(cs, factory)};
    }
};

inline rxcpp::schedulers::scheduler make_srf_scheduler()
{
    static rxcpp::schedulers::scheduler instance = rxcpp::schedulers::make_scheduler<srf_scheduler>();
    return instance;
}

rxcpp::observe_on_one_worker observe_on_srf_scheduler()
{
    static rxcpp::observe_on_one_worker r(make_srf_scheduler());
    return r;
}

std::thread srf_thread_factory(std::function<void()> task);

rxcpp::observe_on_one_worker observe_on_new_srf_thread()
{
    static rxcpp::observe_on_one_worker r(rxcpp::rxsc::make_new_thread(&srf::runnable::srf_thread_factory));
    return r;
}

}  // namespace srf::runnable

namespace rxcpp {
namespace schedulers {
class fiber_pool_scheduler : public scheduler_interface
{
  private:
    typedef fiber_pool_scheduler this_type;
    srf::core::FiberPool& m_pool;

    fiber_pool_scheduler(const this_type&) = delete;

    struct pool_worker : public worker_interface
    {
      private:
        typedef pool_worker this_type;

      public:
        explicit pool_worker(composite_subscription cs, srf::core::FiberPool& pool) : lifetime(cs), m_pool(pool)
        {
            printf("worker %p created\n", this);
        }

        virtual ~pool_worker()
        {
            printf("worker %p destroyed\n", this);
            lifetime.unsubscribe();
        }

        virtual clock_type::time_point now() const override
        {
            return clock_type::now();
        }

        virtual void schedule(const schedulable& scbl) const override
        {
            if (scbl.is_subscribed())
            {
                auto keep_alive = shared_from_this();
                m_pool.enqueue(m_queue_idx++ % m_pool.thread_count(), [keep_alive, scbl]() {
                    // (void)(keep_alive);  // Needed for capture
                    // allow recursion
                    scbl(recursion(true).get_recurse());
                });
            }
        }

        virtual void schedule(clock_type::time_point when, const schedulable& scbl) const override
        {
            if (scbl.is_subscribed())
            {
                auto keep_alive = shared_from_this();
                m_pool.enqueue(m_queue_idx++ % m_pool.thread_count(), [keep_alive, scbl, when]() {
                    // (void)(keep_alive);  // Needed for capture
                    boost::this_fiber::sleep_until(when);
                    // allow recursion
                    scbl(recursion(true).get_recurse());
                });
            }
        }

        composite_subscription lifetime;
        srf::core::FiberPool& m_pool;
        mutable std::atomic_size_t m_queue_idx{0};
    };

  public:
    fiber_pool_scheduler(srf::core::FiberPool& pool) : m_pool(pool) {}

    virtual ~fiber_pool_scheduler() {}

    virtual clock_type::time_point now() const
    {
        return clock_type::now();
    }

    virtual worker create_worker(composite_subscription cs) const
    {
        return worker(cs, std::make_shared<pool_worker>(cs, m_pool));
    }
};

inline scheduler make_fiber_pool(srf::core::FiberPool& pool)
{
    return make_scheduler<fiber_pool_scheduler>(pool);
}
}  // End of namespace schedulers

inline observe_on_one_worker observe_on_fiber_pool(srf::core::FiberPool& pool)
{
    return observe_on_one_worker(rxsc::make_fiber_pool(pool));
}

inline synchronize_in_one_worker synchronize_in_fiber_pool(srf::core::FiberPool& pool)
{
    return synchronize_in_one_worker(rxsc::make_fiber_pool(pool));
}

inline identity_one_worker identitiy_fiber_pool(srf::core::FiberPool& pool)
{
    return identity_one_worker(rxsc::make_fiber_pool(pool));
}

inline serialize_one_worker serialize_fiber_pool(srf::core::FiberPool& pool)
{
    return serialize_one_worker(rxsc::make_fiber_pool(pool));
}
}  // namespace rxcpp
