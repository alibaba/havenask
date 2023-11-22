/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <algorithm>
#include <limits>

#include "indexlib/common_define.h"

namespace indexlib { namespace index {

const uint8_t TRIE_INDEX_VERSION = 1; // for compitable in the feature
const int32_t TRIE_MAX_MATCHES = std::numeric_limits<int32_t>::max();
}} // namespace indexlib::index
