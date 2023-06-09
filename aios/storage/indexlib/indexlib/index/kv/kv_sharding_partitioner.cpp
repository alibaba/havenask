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
#include "indexlib/index/kv/kv_sharding_partitioner.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::document;

using namespace indexlib::util;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, KvShardingPartitioner);

void KvShardingPartitioner::InitKeyHasherType(const IndexPartitionSchemaPtr& schema)
{
    assert(schema->GetTableType() == tt_kv);
    KVIndexConfigPtr kvIndexConfig =
        DYNAMIC_POINTER_CAST(KVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
    assert(kvIndexConfig);
    assert(!kvIndexConfig->GetFieldConfig()->IsMultiValue());
    mKeyHasherType = kvIndexConfig->GetKeyHashFunctionType();
}

}} // namespace indexlib::index
