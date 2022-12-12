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

#pragma once

#include "mrc/node/edge_channel.hpp"
#include "mrc/node/forward.hpp"
#include "mrc/node/source_properties.hpp"

namespace mrc::node {

template <typename T>
class Queue : public IngressProvider<T>, public EgressProvider<T>
{
  public:
    Queue()
    {
        this->set_channel(std::make_unique<mrc::channel::BufferedChannel<T>>());
    }
    ~Queue() override = default;

    void set_channel(std::unique_ptr<mrc::channel::Channel<T>> channel)
    {
        EdgeChannel<T> edge_channel(std::move(channel));

        SinkProperties<T>::init_edge(edge_channel.get_writer());
        SourceProperties<T>::init_edge(edge_channel.get_reader());
    }
};

}  // namespace mrc::node