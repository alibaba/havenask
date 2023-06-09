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
#include "indexlib/index/kkv/multi_region_kkv_reader.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/kkv/codegen_kkv_reader.h"
#include "indexlib/index/kkv/kkv_reader_creator.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MultiRegionKKVReader);

MultiRegionKKVReader::MultiRegionKKVReader() {}

MultiRegionKKVReader::~MultiRegionKKVReader() {}

void MultiRegionKKVReader::Open(const IndexPartitionSchemaPtr& schema, const PartitionDataPtr& partitionData,
                                const SearchCachePartitionWrapperPtr& searchCache, int64_t latestNeedSkipIncTs)
{
    assert(schema);
    mSchema = schema;
    const auto& schemaName = schema->GetSchemaName();

    size_t regionCount = mSchema->GetRegionCount();
    mReaderVec.resize(regionCount);
    if (regionCount > 1) {
        KKVIndexConfigPtr dataKKVConfig = CreateKKVIndexConfigForMultiRegionData(schema);
        KKVReaderPtr reader = KKVReaderCreator::CreateAdaptiveKKVReader(dataKKVConfig, partitionData, searchCache,
                                                                        latestNeedSkipIncTs, schemaName);

        for (regionid_t id = 0; id < (regionid_t)regionCount; id++) {
            mReaderVec[id].reset(reader->Clone());
            KKVIndexConfigPtr kkvConfig =
                DYNAMIC_POINTER_CAST(KKVIndexConfig, schema->GetIndexSchema(id)->GetPrimaryKeyIndexConfig());
            mReaderVec[id]->ReInit(kkvConfig, partitionData, latestNeedSkipIncTs);
        }
    } else {
        assert(regionCount == 1);
        KKVIndexConfigPtr kkvConfig =
            DYNAMIC_POINTER_CAST(KKVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        mReaderVec[0] = KKVReaderCreator::CreateAdaptiveKKVReader(kkvConfig, partitionData, searchCache,
                                                                  latestNeedSkipIncTs, schemaName);
    }
}

const KKVReaderPtr& MultiRegionKKVReader::GetReader(regionid_t regionId) const
{
    if (regionId == INVALID_REGIONID || regionId >= (regionid_t)mReaderVec.size()) {
        return mEmptyReader;
    }
    return mReaderVec[regionId];
}

const KKVReaderPtr& MultiRegionKKVReader::GetReader(const string& regionName) const
{
    assert(mSchema);
    regionid_t regionId = mSchema->GetRegionId(regionName);
    return GetReader(regionId);
}

void MultiRegionKKVReader::SetSearchCachePriority(autil::CacheBase::Priority priority)
{
    for (auto& reader : mReaderVec) {
        if (reader) {
            reader->SetSearchCachePriority(priority);
        }
    }
}

}} // namespace indexlib::index
