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
#include "indexlib/index/kv/kv_factory.h"

#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kv/hash_table_fix_merge_writer.h"
#include "indexlib/index/kv/hash_table_fix_writer.h"
#include "indexlib/index/kv/hash_table_var_merge_writer.h"
#include "indexlib/index/kv/hash_table_var_writer.h"
#include "indexlib/index_base/index_meta/segment_metrics_util.h"
#include "indexlib/index_base/index_meta/segment_topology_info.h"
#include "indexlib/util/KeyHasherTyped.h"

using namespace std;
using namespace indexlib::config;

using namespace indexlib::util;
namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVFactory);

KVTypeId KVFactory::GetKVTypeId(const KVIndexConfigPtr& kvConfig, const KVFormatOptionsPtr& kvOptions)
{
    KVTypeId kvTypeId;
    kvTypeId.hasTTL = (kvConfig->GetTTL() != INVALID_TTL);
    const ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
    if (!kvConfig->GetIndexPreference().GetValueParam().IsFixLenAutoInline()) {
        if (kvConfig->GetRegionCount() == 1 && valueConfig->IsSimpleValue()) {
            kvTypeId.valueType = valueConfig->GetActualFieldType();
            kvTypeId.isVarLen = false;
        } else {
            kvTypeId.isVarLen = true;
        }
    } else {
        if (kvConfig->GetRegionCount() > 1) {
            kvTypeId.isVarLen = true;
        } else {
            int32_t valueSize = valueConfig->GetFixedLength();
            if (valueSize > 8 || valueSize < 0) {
                kvTypeId.isVarLen = true;
            } else {
                FieldType fieldType;
                fieldType = valueConfig->GetFixedLengthValueType();
                if (kvOptions) {
                    kvTypeId.useCompactBucket = kvOptions->UseCompactBucket();
                }
                kvTypeId.valueType = fieldType;
            }
        }
    }
    kvTypeId.multiRegion = (kvConfig->GetRegionCount() > 1);
    FillIndexType(kvConfig, kvTypeId);
    kvTypeId.fileCompress = kvConfig->GetIndexPreference().GetValueParam().EnableFileCompress();
    FillCompactHashFormat(kvConfig, kvOptions, kvTypeId.isVarLen, kvTypeId);
    return kvTypeId;
}

void KVFactory::FillIndexType(const KVIndexConfigPtr& kvConfig, KVTypeId& kvTypeId)
{
    KVIndexPreference preference = kvConfig->GetIndexPreference();
    string hashTypeStr = preference.GetHashDictParam().GetHashType();
    if (hashTypeStr == "dense") {
        kvTypeId.onlineIndexType = indexlibv2::index::KVIndexType::KIT_DENSE_HASH;
        kvTypeId.offlineIndexType = indexlibv2::index::KVIndexType::KIT_DENSE_HASH;
    } else if (hashTypeStr == "cuckoo") {
        kvTypeId.offlineIndexType = indexlibv2::index::KVIndexType::KIT_CUCKOO_HASH;
        kvTypeId.onlineIndexType = indexlibv2::index::KVIndexType::KIT_DENSE_HASH;
    } else {
        // TODO: support
        assert(false);
        IE_LOG(ERROR, "Only support dense or cuckoo hash now, perferType [%d], dictType[%s]", preference.GetType(),
               hashTypeStr.c_str());
        kvTypeId.offlineIndexType = indexlibv2::index::KVIndexType::KIT_DENSE_HASH;
        kvTypeId.onlineIndexType = indexlibv2::index::KVIndexType::KIT_DENSE_HASH;
    }
}

void KVFactory::FillCompactHashFormat(const KVIndexConfigPtr& kvConfig, const KVFormatOptionsPtr& kvOptions,
                                      bool isVarLen, KVTypeId& kvTypeId)
{
    kvTypeId.compactHashKey = false;
    kvTypeId.shortOffset = false;
    if (kvConfig->GetRegionCount() > 1) {
        return;
    }

    KVIndexPreference preference = kvConfig->GetIndexPreference();
    if (isVarLen && preference.GetHashDictParam().HasEnableShortenOffset()) {
        kvTypeId.shortOffset = true;
    }
    if (kvOptions) {
        kvTypeId.shortOffset = kvOptions->IsShortOffset();
    }
    kvTypeId.compactHashKey = kvConfig->IsCompactHashKey();
}

KVWriterPtr KVFactory::CreateWriter(const config::KVIndexConfigPtr& kvIndexConfig)
{
    if (IsVarLenHashTable(kvIndexConfig)) {
        return KVWriterPtr(new HashTableVarWriter());
    } else {
        return KVWriterPtr(new HashTableFixWriter());
    }
}

KVMergeWriterPtr KVFactory::CreateMergeWriter(const config::KVIndexConfigPtr& kvIndexConfig)
{
    if (IsVarLenHashTable(kvIndexConfig)) {
        return KVMergeWriterPtr(new HashTableVarMergeWriter());
    } else {
        return KVMergeWriterPtr(new HashTableFixMergeWriter());
    }
}
}} // namespace indexlib::index
