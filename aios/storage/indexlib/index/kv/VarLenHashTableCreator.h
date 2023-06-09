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

#include "indexlib/index/common/hash_table/HashTableBase.h"

namespace indexlibv2::index {
struct KVTypeId;

class VarLenHashTableCreator
{
public:
    using HashTableInfoPtr = std::unique_ptr<HashTableInfo>;

public:
    static HashTableInfoPtr CreateHashTableForReader(const KVTypeId& typeId, bool useFileReader);
    static HashTableInfoPtr CreateHashTableForWriter(const KVTypeId& typeId);
    static HashTableInfoPtr CreateHashTableForMerger(const KVTypeId& typeId);

private:
    static HashTableInfoPtr InnerCreate(HashTableAccessType accessType, const KVTypeId& typeId);

    template <typename KeyType>
    static HashTableInfoPtr CreateWithKeyType(HashTableAccessType accessType, const KVTypeId& typeId);

    template <typename KeyType, typename ValueType>
    static HashTableInfoPtr CreateWithKeyTypeValueType(HashTableAccessType accessType, const KVTypeId& typeId);
};

} // namespace indexlibv2::index
