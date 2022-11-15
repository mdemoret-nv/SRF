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

#include "internal/control_plane/server/update_issuer.hpp"

#include "srf/node/source_channel.hpp"
#include "srf/protos/architect.pb.h"
#include "srf/utils/macros.hpp"

#include <cstdint>

namespace srf::internal::control_plane::server {

using update_action_t = std::function<srf::protos::ArchitectState(srf::protos::ArchitectState)>;
using update_writer_t = node::SourceChannelWriteable<update_action_t>;

class VersionedState : public UpdateIssuer
{
  public:
    VersionedState() = default;

    DELETE_MOVEABILITY(VersionedState);
    DELETE_COPYABILITY(VersionedState);

    void issue_update(bool force = false) final
    {
        if (m_issued_nonce < m_current_nonce || force)
        {
            m_issued_nonce = m_current_nonce;
            if (has_update() || force)
            {
                auto update = make_update();
                do_issue_update(update);
            }
        }
    }

  protected:
    void mark_as_modified()
    {
        m_current_nonce++;
    }

    const std::size_t& current_nonce() const
    {
        return m_current_nonce;
    }

    protos::StateUpdate make_update() const
    {
        protos::StateUpdate update;
        update.set_service_name(this->service_name());
        update.set_nonce(m_current_nonce);
        do_make_update(update);
        return update;
    }

  private:
    virtual bool has_update() const                                 = 0;
    virtual void do_make_update(protos::StateUpdate& update) const  = 0;
    virtual void do_issue_update(const protos::StateUpdate& update) = 0;

    std::size_t m_current_nonce{1};
    std::size_t m_issued_nonce{1};
};

}  // namespace srf::internal::control_plane::server
