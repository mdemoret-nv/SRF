/*
 * SPDX-FileCopyrightText: Copyright (c) 2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

#include "mrc/edge/edge_holder.hpp"

namespace mrc::edge {

// Declare some explicit template instantiations for common types to speed up compilation
template class EdgeHolder<void*>;
template class EdgeHolder<bool>;
template class EdgeHolder<int>;
template class EdgeHolder<float>;
template class EdgeHolder<double>;
template class EdgeHolder<std::string>;

template class MultiEdgeHolder<size_t, void*>;
template class MultiEdgeHolder<size_t, bool>;
template class MultiEdgeHolder<size_t, int>;
template class MultiEdgeHolder<size_t, float>;
template class MultiEdgeHolder<size_t, double>;
template class MultiEdgeHolder<size_t, std::string>;

}  // namespace mrc::edge
