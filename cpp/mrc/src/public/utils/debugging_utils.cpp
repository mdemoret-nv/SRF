/*
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

#include <fcntl.h>
#include <mrc/utils/debugging_utils.hpp>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cctype>
#include <cstring>

namespace mrc::utils {

bool is_cpp_debugger_attached()
{
    // Adapted from https://stackoverflow.com/a/24969863
    std::array<char, 4096> buf;

    const int status_fd = open("/proc/self/status", O_RDONLY);
    if (status_fd == -1)
    {
        return false;
    }

    const ssize_t num_read = read(status_fd, buf.data(), sizeof(buf) - 1);
    close(status_fd);

    if (num_read <= 0)
    {
        return false;
    }

    buf[num_read]                    = '\0';
    constexpr char TracerPidString[] = "TracerPid:";  // NOLINT(modernize-avoid-c-arrays)
    auto* const tracer_pid_ptr       = strstr(buf.data(), TracerPidString);
    if (tracer_pid_ptr == nullptr)
    {
        return false;
    }

    for (const char* characterPtr = tracer_pid_ptr + sizeof(TracerPidString) - 1; characterPtr <= buf.data() + num_read;
         ++characterPtr)
    {
        if (isspace(*characterPtr) != 0)
        {
            continue;
        }
        return isdigit(*characterPtr) != 0 && *characterPtr != '0';
    }

    return false;
}

}  // namespace mrc::utils
