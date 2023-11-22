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

#include "indexlib/common_define.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, KVReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);

namespace indexlib { namespace index {

class MultiRegionKVReader
{
public:
    MultiRegionKVReader();
    ~MultiRegionKVReader();

public:
    void Open(const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionDataPtr& partitionData,
              const util::SearchCachePartitionWrapperPtr& searchCache, int64_t latestNeedSkipIncTs);

    const KVReaderPtr& GetReader(regionid_t regionId) const;
    const KVReaderPtr& GetReader(const std::string& regionName) const;
    void SetSearchCachePriority(autil::CacheBase::Priority priority);

private:
    KVReaderPtr InnerCreateCodegenReader(config::KVIndexConfigPtr config,
                                         const index_base::PartitionDataPtr& partitionData,
                                         const util::SearchCachePartitionWrapperPtr& searchCache,
                                         int64_t latestNeedSkipIncTs, const std::string& schemaName);

private:
    std::vector<KVReaderPtr> mReaderVec;
    config::IndexPartitionSchemaPtr mSchema;
    KVReaderPtr mEmptyReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiRegionKVReader);
}} // namespace indexlib::index
