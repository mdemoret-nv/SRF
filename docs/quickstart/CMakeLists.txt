# SPDX-FileCopyrightText: Copyright (c) 2022-2024, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

list(APPEND CMAKE_MESSAGE_CONTEXT "quickstart")

cmake_minimum_required(VERSION 3.24 FATAL_ERROR)
set(MRC_RAPIDS_VERSION "24.06" CACHE STRING "Which version of RAPIDS to build for. Sets default versions for RAPIDS CMake and RMM.")

set(MRC_CACHE_DIR "${CMAKE_SOURCE_DIR}/../../cache" CACHE PATH "Directory to contain all CPM and CCache data")
mark_as_advanced(MRC_CACHE_DIR)

list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../../external/utilities/cmake")
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../../cmake")

# Add the build directory to the CMAKE_PREFIX_PATH
list(PREPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../../build")
set(MORPHEUS_UTILS_RAPIDS_VERSION ${MRC_RAPIDS_VERSION})
set(MORPHEUS_UTILS_RAPIDS_CPM_INIT_OVERRIDE "${CMAKE_CURRENT_SOURCE_DIR}/../../cmake/rapids_cpm_package_overrides.json")

include(morpheus_utils/load)

project(mrc-quickstart
  VERSION 24.10.00
  LANGUAGES C CXX
)

morpheus_utils_initialize_cpm(MRC_CACHE_DIR)

# Ensure CPM is initialized
# rapids_cpm_init()

# Set the option prefix to match the outer project before including. Must be before find_package(mrc)
set(OPTION_PREFIX "MRC")

# morpheus_utils_python_configure()

rapids_find_package(mrc REQUIRED)
rapids_find_package(CUDAToolkit REQUIRED)

# To make it easier for CI to find output files, set the default executable suffix to .x if not set
if("${CMAKE_EXECUTABLE_SUFFIX}" STREQUAL "")
  set(CMAKE_EXECUTABLE_SUFFIX ".x")
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(QUICKSTART_HOME "${CMAKE_CURRENT_SOURCE_DIR}")

add_subdirectory(cpp)
# add_subdirectory(hybrid)
# add_subdirectory(python)

list(POP_BACK CMAKE_MESSAGE_CONTEXT)
