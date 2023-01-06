/**
 * SPDX-FileCopyrightText: Copyright (c) 2021-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#include "mrc/core/logging.hpp"

#include <gflags/gflags.h>  // for ParseCommandLineFlags
#include <gtest/gtest.h>    // IWYU pragma: keep
#include <stdnoreturn.h>

// __attribute__((__noreturn__)) void test_failures_throw_exceptions()
// {
//     throw std::runtime_error("exception rather than std::abort");

//     std::abort();
// }

int main(int argc, char** argv)
{
    mrc::init_logging("mrc::test_mrc");
    // ::google::InstallFailureFunction(&test_failures_throw_exceptions);
    ::testing::InitGoogleTest(&argc, argv);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}
