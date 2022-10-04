/*
 * SPDX-FileCopyrightText: Copyright (c) 2022 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#include "./test_srf.hpp"  // IWYU pragma: associated
#include "rxcpp/rx.hpp"

#include "srf/core/executor.hpp"
#include "srf/pipeline/pipeline.hpp"
#include "srf/runnable/rx_thread_factory.hpp"
#include "srf/segment/builder.hpp"
#include "srf/segment/definition.hpp"
#include "srf/utils/type_utils.hpp"

#include <glog/logging.h>
#include <gtest/gtest.h>  // for EXPECT_EQ

#include <chrono>
#include <random>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
TEST_CLASS(RxcppOps);

std::string get_tid()
{
    std::stringstream s;
    s << std::this_thread::get_id();
    return s.str();
}

inline int random_int(int lower, int upper)
{
    std::random_device r;
    std::default_random_engine e1(r());
    std::uniform_int_distribution<int> uniform_dist(lower, upper);
    return uniform_dist(e1);
}

void execute_pipeline(std::unique_ptr<pipeline::Pipeline> pipeline, unsigned short cores)
{
    CHECK(cores > 0);
    std::stringstream cpu_set;
    cpu_set << "0-" << (cores - 1);

    auto options = std::make_unique<Options>();
    options->topology().user_cpuset(cpu_set.str());
    Executor exec(std::move(options));
    exec.register_pipeline(std::move(pipeline));
    exec.start();
    exec.join();
}

TEST_F(TestRxcppOps, concat_map)
{
    auto values = rxcpp::observable<>::range(1, 3).concat_map(
        [](int v) {
            auto st = 1ms * random_int(1, 100);
            return rxcpp::observable<>::interval(std::chrono::steady_clock::now() + st, std::chrono::milliseconds(50))
                .take(3);
        },
        [](int v_main, long v_sub) { return std::make_tuple(v_main, v_sub); });
    values.subscribe([](std::tuple<int, long> v) { printf("OnNext: %d - %ld\n", std::get<0>(v), std::get<1>(v)); },
                     []() { printf("OnCompleted\n"); });
}

TEST_F(TestRxcppOps, flat_map)
{
    auto values = rxcpp::observable<>::range(1, 3).flat_map(
        [](int v) {
            auto st = 1ms * random_int(1, 100);
            return rxcpp::observable<>::interval(std::chrono::steady_clock::now() + st, std::chrono::milliseconds(50))
                .take(3);
        },
        [](int v_main, long v_sub) { return std::make_tuple(v_main, v_sub); });
    values.subscribe([](std::tuple<int, long> v) { printf("OnNext: %d - %ld\n", std::get<0>(v), std::get<1>(v)); },
                     []() { printf("OnCompleted\n"); });
}

TEST_F(TestRxcppOps, threaded_concat_map)
{
    auto values = rxcpp::observable<>::range(1, 3).concat_map(
        [](int v) {
            auto st = 1ms * random_int(1, 100);
            return rxcpp::observable<>::interval(std::chrono::steady_clock::now() + st, std::chrono::milliseconds(50))
                .take(3);
        },
        [](int v_main, long v_sub) { return std::make_tuple(v_main, v_sub); },
        rxcpp::observe_on_new_thread());
    values.as_blocking().subscribe(
        [](std::tuple<int, long> v) {
            printf("[thread %s] OnNext: %d - %ld\n", get_tid().c_str(), std::get<0>(v), std::get<1>(v));
        },
        []() { printf("[thread %s] OnCompleted\n", get_tid().c_str()); });
}

TEST_F(TestRxcppOps, threaded_flat_map)
{
    auto values = rxcpp::observable<>::range(1, 3).flat_map(
        [](int v) {
            auto st = 1ms * random_int(1, 100);
            return rxcpp::observable<>::interval(std::chrono::steady_clock::now() + st, std::chrono::milliseconds(50))
                .take(3);
        },
        [](int v_main, int v_sub) { return std::make_tuple(v_main, v_sub); },
        rxcpp::observe_on_new_thread());
    values.as_blocking().subscribe(
        [](std::tuple<int, long> v) {
            printf("[thread %s] OnNext: %d - %ld\n", get_tid().c_str(), std::get<0>(v), std::get<1>(v));
        },
        []() { printf("[thread %s] OnCompleted\n", get_tid().c_str()); });
}

TEST_F(TestRxcppOps, concat_map_in_segment)
{
    std::size_t iterations = 3;

    auto init = [&iterations](segment::Builder& segment) {
        auto src = segment.make_source<int>("src", [&iterations](rxcpp::subscriber<int> s) {
            for (auto i = 0; i < iterations; ++i)
            {
                VLOG(10) << "src: " << i << std::endl;
                s.on_next(i);
            }
            s.on_completed();
        });

        auto internal_1 = segment.make_node<int, std::string>("internal_1",
                                                              rxcpp::operators::concat_map(
                                                                  [iterations](int v) {
                                                                      auto st = 1ms * random_int(1, 100);
                                                                      return rxcpp::observable<>::interval(
                                                                                 std::chrono::steady_clock::now() + st,
                                                                                 std::chrono::milliseconds(50))
                                                                          .take(iterations);
                                                                  },
                                                                  [](int v1, long v2) {
                                                                      std::stringstream s;
                                                                      s << v1 << " - " << v2;
                                                                      return s.str();
                                                                  }),

                                                              rxcpp::operators::map([](std::string v) {
                                                                  VLOG(10) << "Map: " << v << std::endl;
                                                                  return v;
                                                              }));

        auto sink = segment.make_sink<std::string>(
            "sink",
            [](std::string v) { VLOG(1) << "Sink: " << v << std::endl; },
            []() { VLOG(10) << "Completed" << std::endl; });

        segment.make_edge(src, internal_1);
        segment.make_edge(internal_1, sink);
    };

    auto segdef   = segment::Definition::create("segment_stats_test", init);
    auto pipeline = pipeline::make_pipeline();
    pipeline->register_segment(segdef);
    execute_pipeline(std::move(pipeline), iterations);
}

TEST_F(TestRxcppOps, flat_map_in_segment)
{
    std::size_t iterations = 3;

    auto init = [&iterations](segment::Builder& segment) {
        auto src = segment.make_source<int>("src", [&iterations](rxcpp::subscriber<int> s) {
            for (auto i = 0; i < iterations; ++i)
            {
                VLOG(10) << "src: " << i << std::endl;
                s.on_next(i);
            }
            s.on_completed();
        });

        auto internal_1 = segment.make_node<int, std::string>("internal_1",
                                                              rxcpp::operators::flat_map(
                                                                  [iterations](int v) {
                                                                      auto st = 1ms * random_int(1, 100);
                                                                      return rxcpp::observable<>::interval(
                                                                                 std::chrono::steady_clock::now() + st,
                                                                                 std::chrono::milliseconds(50))
                                                                          .take(iterations);
                                                                  },
                                                                  [](int v1, long v2) {
                                                                      std::stringstream s;
                                                                      s << v1 << " - " << v2;
                                                                      return s.str();
                                                                  }),

                                                              rxcpp::operators::map([](std::string v) {
                                                                  VLOG(10) << "Map: " << v << std::endl;
                                                                  return v;
                                                              }));

        auto sink = segment.make_sink<std::string>(
            "sink",
            [](std::string v) { VLOG(1) << "Sink: " << v << std::endl; },
            []() { VLOG(10) << "Completed" << std::endl; });

        segment.make_edge(src, internal_1);
        segment.make_edge(internal_1, sink);
    };

    auto segdef   = segment::Definition::create("segment_stats_test", init);
    auto pipeline = pipeline::make_pipeline();
    pipeline->register_segment(segdef);
    execute_pipeline(std::move(pipeline), iterations);
}

TEST_F(TestRxcppOps, flat_map_create_in_segment)
{
    std::size_t iterations = 3;

    auto init = [&iterations](segment::Builder& segment) {
        auto src = segment.make_source<int>("src", [&iterations](rxcpp::subscriber<int> s) {
            for (auto i = 0; i < iterations; ++i)
            {
                VLOG(10) << "src: " << i << std::endl;
                s.on_next(i);
            }
            s.on_completed();
        });

        auto internal_1 = segment.make_node<int, std::string>(
            "internal_1",
            rxcpp::operators::flat_map(
                [iterations](int v) {
                    return rxcpp::observable<>::create<int>([iterations, v](rxcpp::subscriber<int> s) {
                        for (int i = 1; i < iterations + 1; ++i)
                        {
                            // auto& context = srf::runnable::Context::get_runtime_context();
                            // bool is_fiber = context.execution_context() == runnable::EngineType::Fiber;
                            auto ri = random_int(1, 100);
                            auto st = 1ms * ri;
                            VLOG(1) << "[" << boost::this_fiber::get_id() << "] (" << v
                                    << ")"  // is_fiber=" << is_fiber
                                    << " Sleeping for: " << ri << "ms" << std::endl;
                            boost::this_fiber::sleep_for(st);
                            VLOG(1) << "[" << boost::this_fiber::get_id() << "] woke: " << v << std::endl;

                            if (s.is_subscribed())
                            {
                                s.on_next(i);
                            }
                        }
                        s.on_completed();
                    });
                },
                [](int v1, long v2) {
                    std::stringstream s;
                    s << v1 << " - " << v2;
                    return s.str();
                },
                srf::runnable::observe_on_new_srf_thread()),

            rxcpp::operators::map([](std::string v) {
                VLOG(10) << "Map: " << v << std::endl;
                return v;
            }));

        auto sink = segment.make_sink<std::string>(
            "sink",
            [](std::string v) { VLOG(1) << "Sink: " << v << std::endl; },
            []() { VLOG(10) << "Completed" << std::endl; });

        segment.make_edge(src, internal_1);
        segment.make_edge(internal_1, sink);
    };

    auto segdef   = segment::Definition::create("segment_stats_test", init);
    auto pipeline = pipeline::make_pipeline();
    pipeline->register_segment(segdef);
    execute_pipeline(std::move(pipeline), iterations);
}
