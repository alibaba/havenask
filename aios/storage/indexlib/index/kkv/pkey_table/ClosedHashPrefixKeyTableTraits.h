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

#include "indexlib/index/common/hash_table/ClosedHashTableTraits.h"
#include "indexlib/index/common/hash_table/CuckooHashTableTraits.h"
#include "indexlib/index/common/hash_table/DenseHashTableTraits.h"
#include "indexlib/index/kkv/Types.h"
#include "indexlib/index/kkv/pkey_table/PKeyTableType.h"

namespace indexlibv2::index {

template <typename ValueType, PKeyTableType Type>
struct ClosedHashPrefixKeyTableTraits;
template <typename ValueType>
struct ClosedHashPrefixKeyTableTraits<ValueType, PKeyTableType::DENSE> {
    using Traits = DenseHashTableTraits<PKeyType, ValueType, false>;
};
template <typename ValueType>
struct ClosedHashPrefixKeyTableTraits<ValueType, PKeyTableType::CUCKOO> {
    using Traits = CuckooHashTableTraits<PKeyType, ValueType, false>;
};

} // namespace indexlibv2::index
