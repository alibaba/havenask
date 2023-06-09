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
#include "indexlib/index/kv/kv_reader.h"

#include "indexlib/config/kv_index_config.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, KVReader);

KVReader::KVReader() : mKeyHasherType(hft_unknown) {}

KVReader::~KVReader() {}

void KVReader::Open(const KVIndexConfigPtr& kvConfig, const PartitionDataPtr& partitionData,
                    const SearchCachePartitionWrapperPtr& searchCache, int64_t latestIncSkipTs)
{
    mKVIndexOptions = KVIndexOptions();
    InitInnerMeta(kvConfig);
    mKVIndexOptions.Init(kvConfig, partitionData, latestIncSkipTs);
}

void KVReader::ReInit(const KVIndexConfigPtr& kvConfig, const PartitionDataPtr& partitionData, int64_t latestIncSkipTs)
{
    mKVIndexOptions = KVIndexOptions();
    InitInnerMeta(kvConfig);
    mKVIndexOptions.Init(kvConfig, partitionData, latestIncSkipTs);
}

void KVReader::InitInnerMeta(const KVIndexConfigPtr& kvConfig)
{
    assert(!kvConfig->GetFieldConfig()->IsMultiValue());
    mKeyHasherType = kvConfig->GetKeyHashFunctionType();

    config::ValueConfigPtr valueConfig = kvConfig->GetValueConfig();
    assert(valueConfig);

    if (valueConfig->GetAttributeCount() == 1) {
        if (kvConfig->GetRegionCount() == 1 && !valueConfig->GetAttributeConfig(0)->IsMultiValue() &&
            (valueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string)) {
            mValueType = indexlibv2::index::KVVT_TYPED;
        } else {
            mValueType = indexlibv2::index::KVVT_PACKED_ONE_FIELD;
        }
    } else {
        mValueType = indexlibv2::index::KVVT_PACKED_MULTI_FIELD;
    }

    if (mValueType != indexlibv2::index::KVVT_TYPED) {
        mPackAttrFormatter.reset(new PackAttributeFormatter);
        if (!mPackAttrFormatter->Init(valueConfig->CreatePackAttributeConfig())) {
            assert(false);
            return;
        }
    }
}
}} // namespace indexlib::index
