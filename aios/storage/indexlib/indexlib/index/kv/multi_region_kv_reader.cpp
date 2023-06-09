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
#include "indexlib/index/kv/multi_region_kv_reader.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/kv/kv_reader_impl.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, MultiRegionKVReader);

MultiRegionKVReader::MultiRegionKVReader() {}

MultiRegionKVReader::~MultiRegionKVReader() {}

void MultiRegionKVReader::Open(const IndexPartitionSchemaPtr& schema, const PartitionDataPtr& partitionData,
                               const SearchCachePartitionWrapperPtr& searchCache, int64_t latestNeedSkipIncTs)
{
    assert(schema);
    mSchema = schema;

    size_t regionCount = mSchema->GetRegionCount();
    mReaderVec.resize(regionCount);
    if (regionCount > 1) {
        KVIndexConfigPtr dataKVConfig = CreateKVIndexConfigForMultiRegionData(schema);
        KVReaderPtr reader = InnerCreateCodegenReader(dataKVConfig, partitionData, searchCache, latestNeedSkipIncTs,
                                                      schema->GetSchemaName());

        for (regionid_t id = 0; id < (regionid_t)regionCount; id++) {
            mReaderVec[id].reset(reader->Clone());
            KVIndexConfigPtr kvConfig =
                DYNAMIC_POINTER_CAST(KVIndexConfig, schema->GetIndexSchema(id)->GetPrimaryKeyIndexConfig());
            mReaderVec[id]->ReInit(kvConfig, partitionData, latestNeedSkipIncTs);
        }
    } else {
        assert(regionCount == 1);
        KVIndexConfigPtr kvConfig =
            DYNAMIC_POINTER_CAST(KVIndexConfig, schema->GetIndexSchema()->GetPrimaryKeyIndexConfig());
        mReaderVec[0] = InnerCreateCodegenReader(kvConfig, partitionData, searchCache, latestNeedSkipIncTs,
                                                 schema->GetSchemaName());
    }
}

KVReaderPtr MultiRegionKVReader::InnerCreateCodegenReader(KVIndexConfigPtr config,
                                                          const PartitionDataPtr& partitionData,
                                                          const SearchCachePartitionWrapperPtr& searchCache,
                                                          int64_t latestNeedSkipIncTs, const std::string& schemaName)
{
    KVReaderPtr reader(new KVReaderImpl);
    reader->Open(config, partitionData, searchCache, latestNeedSkipIncTs);
    auto codegenKVReader = reader->CreateCodegenKVReader();
    if (codegenKVReader) {
        codegenKVReader->Open(config, partitionData, searchCache, latestNeedSkipIncTs);
        reader = codegenKVReader;
    } else {
        IE_LOG(WARN, "create codegen kv reader failed, schema name [%s]", schemaName.c_str());
    }
    return reader;
}

const KVReaderPtr& MultiRegionKVReader::GetReader(regionid_t regionId) const
{
    if (regionId == INVALID_REGIONID || regionId >= (regionid_t)mReaderVec.size()) {
        return mEmptyReader;
    }
    return mReaderVec[regionId];
}

const KVReaderPtr& MultiRegionKVReader::GetReader(const string& regionName) const
{
    assert(mSchema);
    regionid_t regionId = mSchema->GetRegionId(regionName);
    return GetReader(regionId);
}

void MultiRegionKVReader::SetSearchCachePriority(autil::CacheBase::Priority priority)
{
    for (auto& reader : mReaderVec) {
        if (reader) {
            reader->SetSearchCachePriority(priority);
        }
    }
}

}} // namespace indexlib::index
