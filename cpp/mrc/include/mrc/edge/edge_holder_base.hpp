/*
 * SPDX-FileCopyrightText: Copyright (c) 2022-2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#include "mrc/edge/edge.hpp"

#include <algorithm>
#include <memory>
#include <set>
#include <string>

namespace mrc::edge {
class EdgeBase;

struct EdgeHolderBaseState
{
    std::string m_name{""};

    std::vector<EdgeBase*> m_owned_edges;
    std::vector<std::shared_ptr<EdgeBase>> m_connected_edges;
};

class EdgeHolderBase
{
  public:
    EdgeHolderBase() : m_state(std::make_shared<EdgeHolderBaseState>()) {}
    EdgeHolderBase(std::shared_ptr<EdgeHolderBaseState> state) : m_state(state) {}
    virtual ~EdgeHolderBase() = default;

    void set_name(std::string name)
    {
        m_state->m_name = std::move(name);
    }

    const std::string& get_name() const
    {
        return m_state->m_name;
    }

  protected:
    std::shared_ptr<EdgeHolderBaseState> get_state() const
    {
        return m_state;
    }

    void add_owned_edge(EdgeBase* edge)
    {
        CHECK(edge != nullptr) << "Cannot add a null owned edge";

        edge->set_owning_holder(this);

        m_state->m_owned_edges.emplace_back(std::move(edge));
    }

    void remove_owned_edge(EdgeBase* edge)
    {
        CHECK(edge != nullptr) << "Cannot remove a null owned edge";

        auto found = std::find(m_state->m_owned_edges.begin(), m_state->m_owned_edges.end(), edge);

        edge->set_owning_holder(nullptr);

        if (found != m_state->m_owned_edges.end())
        {
            m_state->m_owned_edges.erase(found);
        }
        else
        {
            LOG(WARNING) << "Did not find owned edge in holder: " << this->get_name();
        }
    }

    void add_connected_edge(std::shared_ptr<EdgeBase> edge)
    {
        CHECK(edge) << "Cannot add a null connected edge";

        edge->set_lifetime_holder(this);

        m_state->m_connected_edges.emplace_back(std::move(edge));
    }

    void remove_connected_edge(std::shared_ptr<EdgeBase> edge)
    {
        CHECK(edge) << "Cannot remove a null connected edge";

        auto found = std::find(m_state->m_connected_edges.begin(), m_state->m_connected_edges.end(), edge);

        edge->set_lifetime_holder(nullptr);

        if (found != m_state->m_connected_edges.end())
        {
            m_state->m_connected_edges.erase(found);
        }
        else
        {
            LOG(WARNING) << "Did not find connected edge in holder: " << this->get_name();
        }
    }

    static std::vector<EdgeHolderBase*> walk_lifetime_edges(EdgeHolderBase* current)
    {
        std::vector<EdgeHolderBase*> leafs;

        for (const auto* edge : current->m_state->m_owned_edges)
        {
            // For this edge, see if we have any linked edges
            // edge->m_linked_edges[0].

            // Now get the lifetime holder
            auto* lifetime_holder = edge->m_lifetime_holder;

            if (lifetime_holder == nullptr)
            {
                // This edge is not connected to a lifetime holder, so it is a leaf
                continue;
            }

            auto inner_leafs = walk_lifetime_edges(lifetime_holder);

            leafs.insert(leafs.end(), inner_leafs.begin(), inner_leafs.end());
        }

        if (leafs.empty())
        {
            leafs.push_back(current);
        }

        return leafs;
    }

    //   private:
    //     virtual EdgeHolderBaseState& get_state() = 0;
  private:
    std::shared_ptr<EdgeHolderBaseState> m_state;

    // std::string m_name{""};

    // std::vector<std::weak_ptr<EdgeBase>> m_owned_edges;
    // std::vector<std::shared_ptr<EdgeBase>> m_connected_edges;
};

}  // namespace mrc::edge
