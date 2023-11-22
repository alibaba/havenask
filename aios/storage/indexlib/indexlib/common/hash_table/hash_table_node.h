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

#include <limits>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

const uint32_t INVALID_HASHTABLE_OFFSET = std::numeric_limits<uint32_t>::max();

#pragma pack(push, 4)
template <typename Key, typename Value>
struct HashTableNode {
    Key key;
    Value value;
    uint32_t next;

    HashTableNode(const Key& _key = Key(), const Value& _value = Value(), uint32_t _next = INVALID_HASHTABLE_OFFSET)
        : key(_key)
        , value(_value)
        , next(_next)
    {
    }
};
#pragma pack(pop)
}} // namespace indexlib::common
