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

#include "edge.hpp"

#include "mrc/channel/channel.hpp"
#include "mrc/channel/egress.hpp"
#include "mrc/channel/ingress.hpp"
#include "mrc/edge/edge_holder_base.hpp"
#include "mrc/edge/forward.hpp"
#include "mrc/exceptions/runtime_error.hpp"
#include "mrc/type_traits.hpp"
#include "mrc/utils/string_utils.hpp"

#include <glog/logging.h>
#include <sys/types.h>

#include <cstddef>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <typeindex>
#include <utility>
#include <vector>

namespace mrc::edge {

// EdgeHolder keeps shared pointer of EdgeChannel alive and
template <typename T>
class EdgeHolder : public virtual EdgeHolderBase
{
  public:
    using EdgeHolderBase::EdgeHolderBase;

    virtual ~EdgeHolder()
    {
        // Drop any edge connections before this object goes out of scope. This should execute any disconnectors
        // m_connected_edge.reset();
        this->set_connected_edge(nullptr);

        if (this->check_active_connection(false))
        {
            auto lifetime_leafs = EdgeHolderBase::walk_lifetime_edges(this);

            LOG(FATAL) << "A node was destructed which still had dependent connections. Nodes must be kept alive while "
                          "dependent connections are still active";
        }

        // Drop any owned edges too to erase the pointers
        this->set_owned_edge(nullptr);
    }

  protected:
    bool check_active_connection(bool do_throw = true) const
    {
        // Alive connection exists when the lock is true, lifetime is false or a connction object has been set
        if (m_owned_edge.has_value() && m_owned_edge->lock() && !m_owned_edge_lifetime)
        {
            // Then someone is using this edge already, cant be changed
            if (do_throw)
            {
                throw std::runtime_error("Cant change edge after a connection has been made");
            }
            return true;
        }

        // Check for set connections. Must be connected to throw error
        if (m_connected_edge && m_connected_edge->is_connected())
        {
            // Then someone is using this edge already, cant be changed
            if (do_throw)
            {
                throw std::runtime_error(
                    "Cannot make multiple connections to the same node. Use dedicated Broadcast node");
            }
            return true;
        }

        return false;
    }

    void init_owned_edge(std::shared_ptr<Edge<T>> edge)
    {
        // Check for active connections
        this->check_active_connection();

        EdgeBase* edge_ptr = edge.get();

        // Register a connector to run when edge is connected
        edge->add_connector([this, edge_ptr]() {
            this->add_owned_edge(edge_ptr);

            // Drop the object keeping the weak_edge alive
            this->m_owned_edge_lifetime.reset();
        });

        // Now register a disconnector to keep clean everything up. Only runs if connected
        edge->add_disconnector([this, edge_ptr]() {
            this->remove_owned_edge(edge_ptr);

            this->m_owned_edge_lifetime.reset();
            // this->m_owned_edge.reset();
            this->set_owned_edge(nullptr);
        });

        // Set to the temp edge to ensure its alive until get_edge is called
        m_owned_edge_lifetime = edge;

        // Set to the weak ptr as well
        // m_owned_edge = edge;
        this->set_owned_edge(edge);
    }

    void init_connected_edge(std::shared_ptr<Edge<T>> edge)
    {
        // Check for active connections
        this->check_active_connection();

        // m_connected_edge = edge;
        this->set_connected_edge(edge);
    }

    std::shared_ptr<EdgeHandle> get_edge_connection() const
    {
        if (!m_owned_edge.has_value())
        {
            throw std::runtime_error("Must set an edge before calling get_edge");
        }

        if (auto edge = m_owned_edge->lock())
        {
            return std::shared_ptr<EdgeHandle>(new EdgeHandle(EdgeTypeInfo::create<T>(), edge));
        }

        throw std::runtime_error("The owned edge has expired. This should not happen!");
    }

    void make_edge_connection(std::shared_ptr<EdgeHandle> edge_obj)
    {
        CHECK(edge_obj->get_type() == EdgeTypeInfo::create<T>()) << "Incoming edge connection is not the correct type. "
                                                                    "Make sure to call "
                                                                    "`EdgeBuilder::adapt_writable_edge<T>(edge)` or "
                                                                    "`EdgeBuilder::adapt_readable_edge<T>(edge)` "
                                                                    "before "
                                                                    "calling "
                                                                    "make_edge_connection";

        // Unpack the edge, convert, and call the inner set_edge
        auto unpacked_edge = edge_obj->get_handle_typed<Edge<T>>();

        this->set_edge_handle(unpacked_edge);
    }

    void release_edge_connection()
    {
        // m_connected_edge.reset();
        this->set_connected_edge(nullptr);
    }

    const std::shared_ptr<Edge<T>>& get_connected_edge() const
    {
        return m_connected_edge;
    }

  private:
    void set_edge_handle(std::shared_ptr<Edge<T>> edge)
    {
        // Check for active connections
        this->check_active_connection();

        // Set to the temp edge to ensure its alive until get_edge is called
        // m_connected_edge = edge;
        this->set_connected_edge(edge);

        // Reset the weak_ptr since we dont own this edge
        // m_owned_edge.reset();
        this->set_owned_edge(nullptr);

        // Remove any init lifetime
        m_owned_edge_lifetime.reset();

        // Now indicate that we have a connection
        edge->connect();
    }

    void set_connected_edge(std::shared_ptr<Edge<T>> edge)
    {
        if (m_connected_edge)
        {
            this->remove_connected_edge(m_connected_edge);
        }

        m_connected_edge = edge;

        if (edge)
        {
            this->add_connected_edge(m_connected_edge);
        }
    }

    void set_owned_edge(std::shared_ptr<Edge<T>> edge)
    {
        // if (m_owned_edge.has_value())
        // {
        //     if (auto old_edge = m_owned_edge.value().lock())
        //     {
        //         this->remove_owned_edge(old_edge.get());
        //     }
        //     else
        //     {
        //         LOG(WARNING) << "Could not lock owned edge. Should not happen!";
        //     }
        // }

        m_owned_edge = edge;

        // if (edge)
        // {
        //     this->add_owned_edge(edge.get());
        // }
    }

    // Used for retrieving the current edge without altering its lifetime
    std::optional<std::weak_ptr<Edge<T>>> m_owned_edge;

    // This object ensures that any initialized edge is kept alive and is cleared on connection
    std::shared_ptr<Edge<T>> m_owned_edge_lifetime;

    // Holds a pointer to any set edge (different from init edge). Maintains lifetime
    std::shared_ptr<Edge<T>> m_connected_edge;

    // Allow edge builder to call set_edge
    friend EdgeBuilder;

    // Allow multi edge holder to access protected elements
    template <typename KeyT, typename OtherT>
    friend class MultiEdgeHolder;
};

// template <typename T>
// class FullEdgeHolder : public EdgeHolder<T>
// {
//   public:
//     FullEdgeHolder() : EdgeHolder<T>(m_state) {}

//   private:
//     EdgeHolderBaseState m_state;
// };

template <typename KeyT, typename T>
class MultiEdgeHolder : public virtual EdgeHolderBase
{
  public:
    MultiEdgeHolder()          = default;
    virtual ~MultiEdgeHolder() = default;

  protected:
    void init_owned_edge(KeyT key, std::shared_ptr<Edge<T>> edge)
    {
        auto& edge_pair = this->get_edge_pair(key, true);

        edge_pair.init_owned_edge(std::move(edge));
    }

    std::shared_ptr<EdgeHandle> get_edge_connection(const KeyT& key) const
    {
        auto& edge_pair = this->get_edge_pair(key);

        return edge_pair.get_edge_connection();
    }

    void make_edge_connection(KeyT key, std::shared_ptr<EdgeHandle> edge_obj)
    {
        auto& edge_pair = this->get_edge_pair(key, true);

        edge_pair.make_edge_connection(std::move(edge_obj));
    }

    void release_edge_connection(const KeyT& key)
    {
        auto& edge_pair = this->get_edge_pair(key, true);

        edge_pair.release_edge_connection();

        m_edges.erase(key);
    }

    const std::shared_ptr<Edge<T>>& get_connected_edge(const KeyT& key) const
    {
        auto& edge_pair = this->get_edge_pair(key);

        return edge_pair.get_connected_edge();
    }

    void release_edge_connections()
    {
        for (auto& [key, edge_pair] : m_edges)
        {
            edge_pair.release_edge_connection();
        }

        m_edges.clear();
    }

    size_t edge_connection_count() const
    {
        return m_edges.size();
    }

    std::vector<KeyT> edge_connection_keys() const
    {
        std::vector<KeyT> keys;

        for (const auto& [key, _] : m_edges)
        {
            keys.push_back(key);
        }

        return keys;
    }

    EdgeHolder<T>& get_edge_pair(KeyT key, bool create_if_missing = false)
    {
        auto found = m_edges.find(key);

        if (found == m_edges.end())
        {
            if (create_if_missing)
            {
                m_edges[key] = EdgeHolder<T>(this->get_state());
                return m_edges[key];
            }

            throw std::runtime_error(MRC_CONCAT_STR("Could not find edge pair for key: " << key));
        }

        return found->second;
    }

    EdgeHolder<T>& get_edge_pair(KeyT key)
    {
        auto found = m_edges.find(key);

        if (found == m_edges.end())
        {
            throw std::runtime_error(MRC_CONCAT_STR("Could not find edge pair for key: " << key));
        }

        return found->second;
    }

    const EdgeHolder<T>& get_edge_pair(KeyT key) const
    {
        auto found = m_edges.find(key);

        if (found == m_edges.end())
        {
            throw std::runtime_error(MRC_CONCAT_STR("Could not find edge pair for key: " << key));
        }

        return found->second;
    }

  private:
    void set_edge_handle(KeyT key, std::shared_ptr<Edge<T>> edge)
    {
        auto& edge_pair = this->get_edge_pair(key, true);

        edge_pair.set_edge_handle(std::move(edge));
    }

    // Keeps pairs of get_edge/set_edge for each key
    std::map<KeyT, EdgeHolder<T>> m_edges;

    // Allow edge builder to call set_edge
    friend EdgeBuilder;
};
}  // namespace mrc::edge
