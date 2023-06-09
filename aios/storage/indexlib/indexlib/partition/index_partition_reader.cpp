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
#include "indexlib/partition/index_partition_reader.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/util/KeyHasherTyped.h"

using namespace std;
using namespace autil;

using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexPartitionReader);

index::SummaryReaderPtr IndexPartitionReader::RET_EMPTY_SUMMARY_READER = index::SummaryReaderPtr();
index::SourceReaderPtr IndexPartitionReader::RET_EMPTY_SOURCE_READER = index::SourceReaderPtr();
index::PrimaryKeyIndexReaderPtr IndexPartitionReader::RET_EMPTY_PRIMARY_KEY_READER = index::PrimaryKeyIndexReaderPtr();
index::DeletionMapReaderPtr IndexPartitionReader::RET_EMPTY_DELETIONMAP_READER = index::DeletionMapReaderPtr();
index::InvertedIndexReaderPtr IndexPartitionReader::RET_EMPTY_INDEX_READER = index::InvertedIndexReaderPtr();
index::KKVReaderPtr IndexPartitionReader::RET_EMPTY_KKV_READER = index::KKVReaderPtr();
index::KVReaderPtr IndexPartitionReader::RET_EMPTY_KV_READER = index::KVReaderPtr();

IndexPartitionReader::IndexPartitionReader() {}
IndexPartitionReader::IndexPartitionReader(config::IndexPartitionSchemaPtr schema)
    : mSchema(schema)
    , mTabletSchema(std::make_shared<indexlib::config::LegacySchemaAdapter>(schema))
    , mPartitionVersionHashId(0)
{
}
IndexPartitionReader::~IndexPartitionReader() {}

bool IndexPartitionReader::IsUsingSegment(segmentid_t segmentId) const
{
    return mPartitionVersion->IsUsingSegment(segmentId, mSchema->GetTableType());
}

int64_t IndexPartitionReader::GetLatestDataTimestamp() const
{
    index_base::PartitionDataPtr partData = GetPartitionData();
    if (!partData) {
        return INVALID_TIMESTAMP;
    }
    std::string locatorStr = partData->GetLastLocator();

    index_base::Version incVersion = partData->GetOnDiskVersion();
    OnlineJoinPolicy joinPolicy(incVersion, mSchema->GetTableType(), partData->GetSrcSignature());
    document::IndexLocator indexLocator;
    int64_t incSeekTs = joinPolicy.GetRtSeekTimestampFromIncVersion();

    if (!locatorStr.empty() && !indexLocator.fromString(locatorStr)) {
        return incSeekTs;
    }

    if (indexLocator == document::INVALID_INDEX_LOCATOR) {
        return incSeekTs;
    }
    bool fromInc;
    return OnlineJoinPolicy::GetRtSeekTimestamp(incSeekTs, indexLocator.getOffset(), fromInc);
}

versionid_t IndexPartitionReader::GetIncVersionId() const
{
    return GetPartitionData()->GetOnDiskVersion().GetVersionId();
}

PartitionDataPtr IndexPartitionReader::GetPartitionData() const
{
    assert(false);
    return index_base::PartitionDataPtr();
}

void IndexPartitionReader::InitPartitionVersion(const index_base::PartitionDataPtr& partitionData,
                                                segmentid_t lastLinkSegId)
{
    assert(partitionData);
    index_base::InMemorySegmentPtr inMemSegment = partitionData->GetInMemorySegment();
    segmentid_t buildingSegId = inMemSegment ? inMemSegment->GetSegmentId() : INVALID_SEGMENTID;
    mPartitionVersion = std::make_unique<PartitionVersion>(partitionData->GetVersion(), buildingSegId, lastLinkSegId);
    mPartitionVersionHashId = mPartitionVersion->GetHashId();
}

const index::KKVReaderPtr& IndexPartitionReader::GetKKVReader(regionid_t regionId) const
{
    assert(false);
    return RET_EMPTY_KKV_READER;
}

const index::KKVReaderPtr& IndexPartitionReader::GetKKVReader(const std::string& regionName) const
{
    assert(false);
    return RET_EMPTY_KKV_READER;
}

const index::KVReaderPtr& IndexPartitionReader::GetKVReader(regionid_t regionId) const
{
    assert(false);
    return RET_EMPTY_KV_READER;
}

const index::KVReaderPtr& IndexPartitionReader::GetKVReader(const std::string& regionName) const
{
    assert(false);
    return RET_EMPTY_KV_READER;
}

const table::TableReaderPtr& IndexPartitionReader::GetTableReader() const
{
    static table::TableReaderPtr nullReader;
    return nullReader;
}

bool IndexPartitionReader::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount, size_t wayIdx,
                                                DocIdRangeVector& ranges) const
{
    return false;
}

bool IndexPartitionReader::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
                                                std::vector<DocIdRangeVector>& rangeVectors) const
{
    return false;
}

void IndexPartitionReader::TEST_SetPartitionVersion(std::unique_ptr<PartitionVersion> partitionVersion)
{
    mPartitionVersion = std::move(partitionVersion);
}

indexlibv2::framework::VersionDeployDescriptionPtr IndexPartitionReader::GetVersionDeployDescription() const
{
    auto partitionData = GetPartitionData();
    if (partitionData == nullptr) {
        return nullptr;
    }
    const auto& segDirectory = partitionData->GetSegmentDirectory();
    if (segDirectory == nullptr) {
        return nullptr;
    }
    return segDirectory->GetVersionDeployDescription();
}

}} // namespace indexlib::partition
