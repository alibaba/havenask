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

#include "indexlib/common/hash_table/hash_table_base.h"
#include "indexlib/common/hash_table/special_value.h"
#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/index/kv/kv_format_options.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class HashTableFixCreator
{
public:
    static common::HashTableInfo CreateHashTableForReader(const config::KVIndexConfigPtr& kvIndexConfig,
                                                          bool useFileReader, bool useCompactBucket, bool isRtSegment,
                                                          common::KVMap& nameInfo);

    static common::HashTableInfo CreateHashTableForWriter(const config::KVIndexConfigPtr& kvIndexConfig,
                                                          const KVFormatOptionsPtr& kvOptions, bool isOnline);

private:
    static FieldType GetFieldType(const config::KVIndexConfigPtr& kvIndexConfig);

    template <bool compactBucket>
    static common::HashTableInfo InnerCreateWithCompactBucket(common::HashTableAccessType tableType,
                                                              bool compactHashKey, bool hasTTL, FieldType fieldType,
                                                              common::KVMap& nameInfo);

    template <bool compactBucket, typename KeyType>
    static common::HashTableInfo InnerCreateWithKeyType(common::HashTableAccessType tableType, bool hasTTL,
                                                        FieldType fieldType, common::KVMap& nameInfo);
};

DEFINE_SHARED_PTR(HashTableFixCreator);
}} // namespace indexlib::index
