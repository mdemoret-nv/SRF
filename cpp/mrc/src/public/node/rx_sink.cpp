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

#include "mrc/node/rx_sink.hpp"

namespace mrc::node {

// Declare some explicit template instantiations for common types to speed up compilation
template class RxSink<void*>;
template class RxSink<bool>;
template class RxSink<int>;
template class RxSink<float>;
template class RxSink<double>;
template class RxSink<std::string>;

template class RxSinkComponent<void*>;
template class RxSinkComponent<bool>;
template class RxSinkComponent<int>;
template class RxSinkComponent<float>;
template class RxSinkComponent<double>;
template class RxSinkComponent<std::string>;

}  // namespace mrc::node
