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

#include "internal/runnable/resources.hpp"
#include "internal/system/resources.hpp"
#include "internal/system/system.hpp"
#include "internal/system/system_provider.hpp"

#include "srf/core/bitmap.hpp"
#include "srf/core/executor.hpp"
#include "srf/options/engine_groups.hpp"
#include "srf/options/options.hpp"
#include "srf/options/topology.hpp"
#include "srf/pipeline/pipeline.hpp"
#include "srf/runnable/context.hpp"
#include "srf/runnable/fiber_context.hpp"
#include "srf/runnable/forward.hpp"
#include "srf/runnable/launch_control.hpp"
#include "srf/runnable/launch_options.hpp"
#include "srf/runnable/launcher.hpp"
#include "srf/runnable/runnable.hpp"
#include "srf/runnable/runner.hpp"
#include "srf/runnable/thread_context.hpp"
#include "srf/runnable/type_traits.hpp"
#include "srf/runnable/types.hpp"

#include <boost/fiber/operations.hpp>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <hwloc.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

using namespace srf;

#define SRF_DEFAULT_FIBER_PRIORITY 0

static std::shared_ptr<internal::system::System> make_system(std::function<void(Options&)> updater = nullptr)
{
    auto options = std::make_shared<Options>();
    if (updater)
    {
        updater(*options);
    }

    return internal::system::make_system(std::move(options));
}

class TestRunnable : public ::testing::Test
{
  protected:
    virtual runnable::EngineType get_defaul_engine_type() const
    {
        return runnable::EngineType::Fiber;
    }

    void SetUp() override
    {
        m_system_resources = std::make_unique<internal::system::Resources>(
            internal::system::SystemProvider(make_system([this](Options& options) {
                options.topology().user_cpuset("0-3");
                options.topology().restrict_gpus(true);
                options.engine_factories().set_default_engine_type(this->get_defaul_engine_type());

                this->configure_engines(options.engine_factories());
            })));

        m_resources = std::make_unique<internal::runnable::Resources>(*m_system_resources, 0);
    }

    virtual void configure_engines(srf::EngineGroups& engine_groups)
    {
        // Nothing in base
    }

    void TearDown() override
    {
        m_resources.reset();
        m_system_resources.reset();
    }

    std::unique_ptr<internal::system::Resources> m_system_resources;
    std::unique_ptr<internal::runnable::Resources> m_resources;
};

class TestRunnableThreadPool : public TestRunnable
{
  protected:
    void configure_engines(srf::EngineGroups& engine_groups) override
    {
        engine_groups.set_engine_factory_options("thread_pool", [](EngineFactoryOptions& options) {
            options.engine_type   = runnable::EngineType::Thread;
            options.allow_overlap = true;
            options.cpu_count     = 2;
        });
    }
};

class TestGenericRunnable final : public runnable::RunnableWithContext<>
{
    void run(ContextType& ctx) final
    {
        LOG(INFO) << info(ctx) << ": do_run begin";
        auto status = state();
        while (status == State::Run)
        {
            boost::this_fiber::sleep_for(std::chrono::milliseconds(50));
            status = state();
        }
        LOG(INFO) << info(ctx) << ": do_run end";
    }
};

class TestFiberRunnable final : public runnable::FiberRunnable<>
{
    void run(ContextType& ctx) final
    {
        LOG(INFO) << info(ctx) << ": do_run begin";
        auto status = state();
        while (status == State::Run)
        {
            boost::this_fiber::sleep_for(std::chrono::milliseconds(50));
            status = state();
        }
        LOG(INFO) << info(ctx) << ": do_run end";
    }

  public:
    int i;
};

class TestThreadRunnable final : public runnable::ThreadRunnable<>
{
    void run(ContextType& ctx) final
    {
        LOG(INFO) << info(ctx) << ": do_run begin";
        auto status = state();
        while (status == State::Run)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            status = state();
        }
        LOG(INFO) << info(ctx) << ": do_run end";
    }
};

template <typename ContextT = runnable::Context>
class TestLambdaRunnable : public runnable::RunnableWithContext<ContextT>
{
  public:
    TestLambdaRunnable(std::function<void(ContextT& ctx)> lambda) : m_lambda(std::move(lambda)) {}

  private:
    void run(ContextT& ctx) override
    {
        this->m_lambda(ctx);
    }

    std::function<void(ContextT& ctx)> m_lambda;
};

TEST_F(TestRunnable, TypeTraitsGeneric)
{
    using ctx_t = runnable::runnable_context_t<TestGenericRunnable>;
    static_assert(std::is_same_v<ctx_t, runnable::Context>, "should be true");
    static_assert(runnable::is_unwrapped_context_v<ctx_t>, "should be true");
    static_assert(!runnable::is_fiber_runnable_v<TestGenericRunnable>, "should be false");
    static_assert(!runnable::is_fiber_context_v<ctx_t>, "should be false");
    static_assert(!runnable::is_thread_context_v<ctx_t>, "should be false");
    static_assert(std::is_same_v<runnable::unwrap_context_t<ctx_t>, runnable::Context>, "true");
}

TEST_F(TestRunnable, TypeTraitsFiber)
{
    using ctx_t = runnable::runnable_context_t<TestFiberRunnable>;
    static_assert(runnable::is_fiber_runnable_v<TestFiberRunnable>, "should be true");
    static_assert(runnable::is_fiber_context_v<ctx_t>, "should be true");
    static_assert(std::is_same_v<runnable::unwrap_context_t<ctx_t>, runnable::Context>, "true");
}

TEST_F(TestRunnable, TypeTraitsThread)
{
    using ctx_t = runnable::runnable_context_t<TestThreadRunnable>;
    static_assert(!runnable::is_fiber_runnable_v<TestThreadRunnable>, "should be false");
    static_assert(runnable::is_thread_context_v<ctx_t>, "should be true");
    static_assert(std::is_same_v<runnable::unwrap_context_t<ctx_t>, runnable::Context>, "true");
}

TEST_F(TestRunnable, GenericRunnableRunWithFiber)
{
    runnable::LaunchOptions main;
    std::atomic<std::size_t> counter = 0;

    main.set_engine_factory_name("default");  // running this on main would fail, since main can only have pe_count == 1
    main.set_counts(2);

    auto runnable = std::make_unique<TestGenericRunnable>();
    auto launcher = m_resources->launch_control().prepare_launcher(main, std::move(runnable));

    launcher->apply([&counter](runnable::Runner& runner) {
        runner.on_instance_state_change_callback([&counter](const runnable::Runnable& runnable,
                                                            std::size_t id,
                                                            runnable::Runner::State old_state,
                                                            runnable::Runner::State new_state) { ++counter; });
    });

    auto runner = launcher->ignition();

    runner->await_live();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    runner->stop();
    runner->await_join();

    // counter should be 3 for each instance
    // Queued, Running, Completed
    EXPECT_EQ(counter, 3 * main.pe_count() * main.engines_per_pe());
}

TEST_F(TestRunnable, GenericRunnableRunWithLaunchControl)
{
    auto runnable = std::make_unique<TestGenericRunnable>();

    auto runner = m_resources->launch_control().prepare_launcher(std::move(runnable))->ignition();

    runner->await_live();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    runner->stop();
    runner->await_join();
}

TEST_F(TestRunnableThreadPool, GenericRunnableRunWithThread)
{
    runnable::LaunchOptions thread_pool;
    thread_pool.set_engine_factory_name("thread_pool");
    thread_pool.set_counts(2);

    auto runnable = std::make_unique<TestGenericRunnable>();
    auto runner   = m_resources->launch_control().prepare_launcher(thread_pool, std::move(runnable))->ignition();

    runner->await_live();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    runner->stop();
    runner->await_join();
}

TEST_F(TestRunnable, FiberRunnable)
{
    runnable::LaunchOptions factory;
    factory.set_engine_factory_name("default");
    factory.set_counts(2);

    auto runnable = std::make_unique<TestFiberRunnable>();
    auto runner   = m_resources->launch_control().prepare_launcher(factory, std::move(runnable))->ignition();

    runner->await_live();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    runner->stop();
    runner->await_join();
}

TEST_F(TestRunnableThreadPool, FiberRunnableMisMatch)
{
    runnable::LaunchOptions factory;
    factory.set_engine_factory_name("thread_pool");
    factory.set_counts(2);

    auto runnable = std::make_unique<TestFiberRunnable>();

    // todo(#188) - this should throw an exception rather than abort
    EXPECT_DEATH(auto launcher = m_resources->launch_control().prepare_launcher(factory, std::move(runnable)), "");
}

TEST_F(TestRunnable, RunnerOutOfScope)
{
    runnable::LaunchOptions factory;
    factory.set_engine_factory_name("default");
    factory.set_counts(2);

    auto runnable = std::make_unique<TestFiberRunnable>();
    auto runner   = m_resources->launch_control().prepare_launcher(factory, std::move(runnable))->ignition();

    runner->await_live();
}

TEST_F(TestRunnable, PEandWorkerCounts)
{
    std::atomic<std::size_t> counter = 0;

    runnable::LaunchOptions fibers_options;

    // Test name
    fibers_options.set_engine_factory_name("fibers");
    EXPECT_EQ(fibers_options.engine_factory_name(), "fibers");

    // Test setting counts without worker count
    fibers_options.set_counts(3);
    EXPECT_EQ(fibers_options.pe_count(), 3);
    EXPECT_EQ(fibers_options.worker_count(), 3);
    EXPECT_EQ(fibers_options.engines_per_pe(), 1);

    // Test setting both PE and workers
    fibers_options.set_counts(4, 4);
    EXPECT_EQ(fibers_options.pe_count(), 4);
    EXPECT_EQ(fibers_options.worker_count(), 4);
    EXPECT_EQ(fibers_options.engines_per_pe(), 1);

    // Test setting PE > Workers
    fibers_options.set_counts(6, 2);
    EXPECT_EQ(fibers_options.pe_count(), 6);
    EXPECT_EQ(fibers_options.worker_count(), 2);
    EXPECT_EQ(fibers_options.engines_per_pe(), 3);

    // Test setting both workers > PE
    fibers_options.set_counts(1, 5);
    EXPECT_EQ(fibers_options.pe_count(), 1);
    EXPECT_EQ(fibers_options.worker_count(), 5);
    EXPECT_EQ(fibers_options.engines_per_pe(), 1);
}

class LaunchOptionsSuite
  : public TestRunnable,
    public testing::WithParamInterface<std::tuple<runnable::EngineType, std::size_t, std::size_t>>
{
  public:
    runnable::EngineType engine_type() const
    {
        return std::get<0>(GetParam());
    }

    std::size_t pe_count() const
    {
        return std::get<1>(GetParam());
    }

    std::size_t worker_count() const
    {
        return std::get<2>(GetParam());
    }

    std::size_t pe_per_worker() const
    {
        return std::max(this->pe_count() / this->worker_count(), 1UL);
    }

  protected:
    // Override the engine type based on the parameter
    runnable::EngineType get_defaul_engine_type() const override
    {
        return this->engine_type();
    }

    void track_thread_id()
    {
        std::lock_guard<decltype(m_mutex)> lock(m_mutex);

        auto id = std::hash<std::thread::id>()(std::this_thread::get_id());

        // Get the current topology
        hwloc_topology_t topology;
        hwloc_topology_init(&topology);
        hwloc_topology_load(topology);

        hwloc_cpuset_t cpu_set = hwloc_bitmap_alloc();
        // hwloc_get_last_cpu_location(topology, cpu_set, HWLOC_CPUBIND_THREAD);
        auto ret = hwloc_get_cpubind(topology, cpu_set, HWLOC_CPUBIND_THREAD);

        // Now make a bitmap from this
        Bitmap bitmap(cpu_set);

        // Get the vector of CPU IDs
        auto cpu_ids = bitmap.vec();

        for (auto cpu_id : cpu_ids)
        {
            m_cpuid_thread_ids[cpu_id].insert(id);
            m_cpuid_thread_counts[cpu_id]++;
        }

        hwloc_bitmap_free(cpu_set);
        hwloc_topology_destroy(topology);

        m_unique_thread_ids.insert(id);
    }

    std::mutex m_mutex;
    std::set<std::size_t> m_unique_thread_ids;
    std::map<std::uint32_t, std::set<std::size_t>> m_cpuid_thread_ids;
    std::map<std::uint32_t, std::size_t> m_cpuid_thread_counts;
};

TEST_P(LaunchOptionsSuite, CheckEngineThreads)
{
    auto worker_count = this->worker_count();

    runnable::LaunchOptions factory;
    factory.set_engine_factory_name("default");
    factory.set_counts(this->pe_count(), worker_count);

    auto runnable_body = [this](runnable::Context& ctx) { this->track_thread_id(); };

    std::unique_ptr<srf::runnable::Runner> runner = nullptr;

    if (this->engine_type() == runnable::EngineType::Fiber)
    {
        auto runnable = std::make_unique<TestLambdaRunnable<runnable::FiberContext<>>>(runnable_body);
        runner        = m_resources->launch_control().prepare_launcher(factory, std::move(runnable))->ignition();
    }
    else
    {
        auto runnable = std::make_unique<TestLambdaRunnable<runnable::ThreadContext<>>>(runnable_body);
        runner        = m_resources->launch_control().prepare_launcher(factory, std::move(runnable))->ignition();
    }

    runner->await_live();
    runner->await_join();

    // Make sure that we have the right thread count. Will be either the number of PE count (if PE < Workers) or the
    // Worker count (if PE > Workers)
    EXPECT_EQ(m_cpuid_thread_ids.size(), std::min(this->worker_count(), this->pe_count()));

    std::size_t threads_per_cpuid = this->pe_per_worker();

    // If we are using fibers, there will only ever be 1 thread per core
    if (this->engine_type() == runnable::EngineType::Fiber)
    {
        threads_per_cpuid = 1;
    }

    // Check for the right number of PE per thread
    for (auto const& pair : m_cpuid_thread_ids)
    {
        EXPECT_EQ(pair.second.size(), threads_per_cpuid);
    }

    // Check for the right number of calls per core
    for (auto const& pair : m_cpuid_thread_counts)
    {
        EXPECT_EQ(pair.second, this->pe_per_worker());
    }
}

TEST_P(LaunchOptionsSuite, CheckEngineTaskThreads)
{
    auto worker_count = this->worker_count();

    runnable::LaunchOptions factory;
    factory.set_engine_factory_name("default");
    factory.set_counts(this->pe_count(), worker_count);

    auto runnable_body = [=](runnable::Context& ctx) {
        std::vector<Future<void>> futures;

        // Launch some additional tasks
        for (size_t i = 0; i < worker_count; ++i)
        {
            futures.push_back(ctx.engine()->run_task([this]() {
                // Track this thread
                this->track_thread_id();
            }));
        }

        for (auto& f : futures)
        {
            f.wait();
        }
    };

    std::unique_ptr<srf::runnable::Runner> runner = nullptr;

    if (this->engine_type() == runnable::EngineType::Fiber)
    {
        auto runnable = std::make_unique<TestLambdaRunnable<runnable::FiberContext<>>>(runnable_body);
        runner        = m_resources->launch_control().prepare_launcher(factory, std::move(runnable))->ignition();
    }
    else
    {
        auto runnable = std::make_unique<TestLambdaRunnable<runnable::ThreadContext<>>>(runnable_body);
        runner        = m_resources->launch_control().prepare_launcher(factory, std::move(runnable))->ignition();
    }

    runner->await_live();
    runner->await_join();

    // Make sure that we have the right thread count. Will be the worker count
    EXPECT_EQ(m_cpuid_thread_ids.size(), this->worker_count());

    std::size_t threads_per_cpuid = this->pe_count();

    // If we are using fibers, there will only ever be 1 thread per core
    if (this->engine_type() == runnable::EngineType::Fiber)
    {
        threads_per_cpuid = 1;
    }

    // Check for the right number of PE per thread
    for (auto const& pair : m_cpuid_thread_ids)
    {
        EXPECT_EQ(pair.second.size(), threads_per_cpuid);
    }

    // Check for the right number of calls per core
    for (auto const& pair : m_cpuid_thread_counts)
    {
        EXPECT_EQ(pair.second, this->pe_count());
    }
}

INSTANTIATE_TEST_SUITE_P(TestRunnable,
                         LaunchOptionsSuite,
                         testing::Combine(testing::Values(runnable::EngineType::Fiber, runnable::EngineType::Thread),
                                          testing::Values(1, 2, 4),
                                          testing::Values(1, 2, 4)),
                         [](const testing::TestParamInfo<LaunchOptionsSuite::ParamType>& info) {
                             return SRF_CONCAT_STR(
                                 (std::get<0>(info.param) == runnable::EngineType::Fiber ? "Fiber" : "Thread")
                                 << "PE" << std::get<1>(info.param) << "Workers" << std::get<2>(info.param));
                         });

// Move the remaining tests to TestNode

// TEST_F(TestRunnable, ThreadRunnable)
// {
//     CpuSet cpus("0,1");

//     auto runnable = std::make_unique<TestThreadRunnable>();
//     auto runner   = runnable::make_runner(std::move(runnable));
//     auto launcher = std::make_shared<runnable::ThreadEngines>(cpus, m_topology);

//     runner->enqueue(launcher);
//     runner->await_live();
//     std::this_thread::sleep_for(std::chrono::milliseconds(50));
//     runner->stop();
//     runner->await_join();
// }

// TEST_F(TestRunnable, OperatorMuxer)
// {
//     std::atomic<std::size_t> counter = 0;
//     std::unique_ptr<runnable::Runner> runner_source;
//     std::unique_ptr<runnable::Runner> runner_sink;

//     // do the construction in its own scope
//     // only allow the runners to escape the scope
//     // this ensures that the Muxer Operator survives
//     {
//         auto source =
//             std::make_unique<node::RxSource<float>>(rxcpp::observable<>::create<float>([](rxcpp::subscriber<float> s)
//             {
//                 s.on_next(1.0f);
//                 s.on_next(2.0f);
//                 s.on_next(3.0f);
//                 s.on_completed();
//             }));
//         auto muxer = std::make_shared<node::Muxer<float>>();
//         auto sink =
//             std::make_unique<node::RxSink<float>>(rxcpp::make_observer_dynamic<float>([&](float x) { ++counter; }));

//         node::make_edge(*source, *muxer);
//         node::make_edge(*muxer, *sink);

//         runner_sink   = m_launch_control->prepare_launcher(std::move(sink))->ignition();
//         runner_source = m_launch_control->prepare_launcher(std::move(source))->ignition();
//     }

//     runner_source->await_join();
//     runner_sink->await_join();

//     EXPECT_EQ(counter, 3);
// }

// TEST_F(TestRunnable, IdentityNode)
// {
//     std::atomic<std::size_t> counter = 0;
//     std::unique_ptr<runnable::Runner> runner_source;
//     std::unique_ptr<runnable::Runner> runner_passthru;
//     std::unique_ptr<runnable::Runner> runner_sink;

//     // do the construction in its own scope
//     // only allow the runners to escape the scope
//     // this ensures that the Muxer Operator survives
//     {
//         auto source =
//             std::make_unique<node::RxSource<float>>(rxcpp::observable<>::create<float>([](rxcpp::subscriber<float> s)
//             {
//                 s.on_next(1.0f);
//                 s.on_next(2.0f);
//                 s.on_next(3.0f);
//                 s.on_completed();
//             }));
//         auto passthru = std::make_unique<node::RxNode<float>>(
//             rxcpp::operators::tap([this](const float& t) { LOG(INFO) << "tap = " << t; }));
//         auto sink =
//             std::make_unique<node::RxSink<float>>(rxcpp::make_observer_dynamic<float>([&](float x) { ++counter; }));

//         node::make_edge(*source, *passthru);
//         node::make_edge(*passthru, *sink);

//         runner_sink     = m_launch_control->prepare_launcher(std::move(sink))->ignition();
//         runner_passthru = m_launch_control->prepare_launcher(std::move(passthru))->ignition();
//         runner_source   = m_launch_control->prepare_launcher(std::move(source))->ignition();
//     }

//     runner_source->await_join();
//     runner_passthru->await_join();
//     runner_sink->await_join();

//     EXPECT_EQ(counter, 3);
// }
