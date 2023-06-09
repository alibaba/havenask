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

#include "autil/Log.h"
#include "autil/LongHashValue.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::index {

template <typename KeyType, typename ValueType>
struct PKPair {
    KeyType key;
    ValueType docid;
    inline bool operator<(const PKPair& rhs) const { return key < rhs.key; }
    inline bool operator<(const KeyType& rhs) const { return key < rhs; }
    inline bool operator==(const PKPair& rhs) const { return key == rhs.key; }
    inline bool operator==(const KeyType& rhs) const { return key == rhs; }
};

// The packing size used for class template instantiation is set
// by the #pragma pack directive which is in effect at the point
// of template instantiation (whether implicit or explicit),
// rather then by the #pragma pack directive which was in effect
// at the point of class template definition.

// so we need to explicit the template instantiation here to ensure
// that the packing size of PKPair is same as we expected

#pragma pack(push)
#pragma pack(4)
template <>
struct PKPair<autil::uint128_t, docid_t> {
    autil::uint128_t key;
    docid_t docid;
    inline bool operator<(const PKPair& rhs) const { return key < rhs.key; }
    inline bool operator<(const autil::uint128_t& rhs) const { return key < rhs; }
    inline bool operator==(const PKPair& rhs) const { return key == rhs.key; }
    inline bool operator==(const autil::uint128_t& rhs) const { return key == rhs; }
};

template <>
struct PKPair<uint64_t, docid_t> {
    uint64_t key;
    docid_t docid;
    inline bool operator<(const PKPair& rhs) const { return key < rhs.key; }
    inline bool operator<(const uint64_t& rhs) const { return key < rhs; }
    inline bool operator==(const PKPair& rhs) const { return key == rhs.key; }
    inline bool operator==(const uint64_t& rhs) const { return key == rhs; }
};
#pragma pack(pop)
} // namespace indexlibv2::index
