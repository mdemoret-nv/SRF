/**
 * SPDX-FileCopyrightText: Copyright (c) 2022, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#include "srf/codable/api.hpp"
#include "srf/codable/encoded_object.hpp"
#include "srf/node/source_channel.hpp"

#include <string>

namespace srf::pubsub {

struct IService
{
    virtual ~IService() = default;

    virtual const std::string& service_name() const = 0;
    virtual const std::uint64_t& tag() const        = 0;
};

class IPublisher : public IService, public node::SourceChannelWriteable<std::unique_ptr<srf::codable::EncodedStorage>>
{
  public:
    using elemnent_type = std::unique_ptr<srf::codable::EncodedStorage>;

    ~IPublisher() override                                             = default;
    virtual std::unique_ptr<codable::ICodableStorage> create_storage() = 0;
};

class ISubscriber : public IService, public node::SinkProperties<std::unique_ptr<codable::IDecodableStorage>>
{};

}  // namespace srf::pubsub