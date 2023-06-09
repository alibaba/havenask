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

#include <memory>

#include "autil/LongHashValue.h"
#include "indexlib/base/Types.h"

namespace indexlib::index {

template <typename Key, typename Value>
struct KeyValueItem {
    Key key;
    Value value;
    bool friend operator<(const KeyValueItem& left, const KeyValueItem& right)
    {
        return left.key != right.key ? left.key < right.key : left.value < right.value;
    }
    bool friend operator<(const Key& key, const KeyValueItem& right) { return key < right.key; }
    bool friend operator<(const KeyValueItem& left, const Key& key) { return left.key < key; }
    bool friend operator==(const KeyValueItem& left, const KeyValueItem& right)
    {
        return left.key == right.key && left.value == right.value;
    }
} __attribute__((packed));

template <typename Key, typename Value>
struct KeyValueItemUnpack {
    Key key;
    Value value;
};

#pragma pack(push)
#pragma pack(4)
template <>
struct KeyValueItem<autil::uint128_t, docid_t> {
    autil::uint128_t key;
    docid_t value;
    bool friend operator<(const KeyValueItem& left, const KeyValueItem& right)
    {
        return left.key != right.key ? left.key < right.key : left.value < right.value;
    }
    bool friend operator<(const autil::uint128_t& key, const KeyValueItem& right) { return key < right.key; }
    bool friend operator<(const KeyValueItem& left, const autil::uint128_t& key) { return left.key < key; }
    bool friend operator==(const KeyValueItem& left, const KeyValueItem& right)
    {
        return left.key == right.key && left.value == right.value;
    }
};

#pragma pack(pop)
} // namespace indexlib::index
