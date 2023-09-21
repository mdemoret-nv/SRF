/*
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

#include "mrc/channel/status.hpp"
#include "mrc/edge/deferred_edge.hpp"  // IWYU pragma: export
#include "mrc/edge/edge_holder.hpp"    // IWYU pragma: export
#include "mrc/edge/edge_writable.hpp"  // IWYU pragma: export
#include "mrc/edge/forward.hpp"        // IWYU pragma: keep
#include "mrc/type_traits.hpp"
#include "mrc/utils/type_utils.hpp"

#include <glog/logging.h>

#include <cstddef>
#include <memory>  // IWYU pragma: export
#include <sstream>
#include <typeindex>
#include <utility>
#include <vector>  // IWYU pragma: export

namespace mrc::edge {

template <typename T>
class DeferredWritableMultiEdge : public MultiEdgeHolder<std::size_t, T>,
                                  public IEdgeWritable<T>,
                                  public DeferredWritableMultiEdgeBase
{
  public:
    DeferredWritableMultiEdge(determine_indices_fn_t indices_fn = nullptr, bool deep_copy = false) :
      m_indices_fn(std::move(indices_fn))
    {
        // // Generate warning if deep_copy = True but type does not support it
        // if constexpr (!std::is_copy_constructible_v<T>)
        // {
        //     if (m_deep_copy)
        //     {
        //         LOG(WARNING) << "DeferredWritableMultiEdge(deep_copy=True) created for type '" << type_name<T>()
        //                      << "' but the type is not copyable. Deep copy will be disabled";

        //         m_deep_copy = false;
        //     }
        // }

        // Set a connector to check that the indices function has been set
        this->add_connector([this]() {
            // Ensure that the indices function is properly set
            CHECK(this->m_indices_fn) << "Must set indices function before connecting edge";
        });
    }

    channel::Status await_write(T&& data) override
    {
        auto indices = this->determine_indices_for_value(data);

        // First, handle the situation where there is more than one connection to push to
        if constexpr (!std::is_copy_constructible_v<T>)
        {
            CHECK(indices.size() <= 1) << type_name<DeferredWritableMultiEdge<T>>()
                                       << " is trying to write to multiple downstreams but the object type is not "
                                          "copyable. Must use copyable type with multiple downstream connections";
        }
        else
        {
            for (size_t i = indices.size() - 1; i > 0; --i)
            {
                // if constexpr (is_shared_ptr<T>::value)
                // {
                //     if (m_deep_copy)
                //     {
                //         auto deep_copy = std::make_shared<typename T::element_type>(*data);
                //         CHECK(this->get_writable_edge(indices[i])->await_write(std::move(deep_copy)) ==
                //               channel::Status::success);
                //         continue;
                //     }
                // }

                T shallow_copy(data);
                CHECK(this->get_writable_edge(indices[i])->await_write(std::move(shallow_copy)) ==
                      channel::Status::success);
            }
        }

        // Always push the last one the same way
        if (indices.size() >= 1)
        {
            return this->get_writable_edge(indices[0])->await_write(std::move(data));
        }

        return channel::Status::success;
    }

    void set_indices_fn(determine_indices_fn_t indices_fn) override
    {
        m_indices_fn = std::move(indices_fn);
    }

    size_t edge_connection_count() const override
    {
        return MultiEdgeHolder<std::size_t, T>::edge_connection_count();
    }
    std::vector<std::size_t> edge_connection_keys() const override
    {
        return MultiEdgeHolder<std::size_t, T>::edge_connection_keys();
    }

  protected:
    std::shared_ptr<IEdgeWritable<T>> get_writable_edge(std::size_t edge_idx) const
    {
        return std::dynamic_pointer_cast<IEdgeWritable<T>>(this->get_connected_edge(edge_idx));
    }

    virtual std::vector<std::size_t> determine_indices_for_value(const T& data)
    {
        return m_indices_fn(*this);
    }

  private:
    void set_writable_edge_handle(std::size_t key, std::shared_ptr<WritableEdgeHandle> ingress) override;

    bool m_deep_copy{false};
    determine_indices_fn_t m_indices_fn{};
};

}  // namespace mrc::edge

#include "mrc/edge/edge_builder.hpp"

template <typename T>
void mrc::edge::DeferredWritableMultiEdge<T>::set_writable_edge_handle(std::size_t key,
                                                                       std::shared_ptr<WritableEdgeHandle> ingress)
{
    // Do any conversion to the correct type here
    auto adapted_ingress = EdgeBuilder::adapt_writable_edge<T>(ingress);

    MultiEdgeHolder<std::size_t, T>::make_edge_connection(key, adapted_ingress);
}
