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
#include "indexlib/partition/realtime_partition_data_reclaimer_base.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/inverted_index/KeyIterator.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/framework/legacy_index_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/realtime_partition_data_reclaimer.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RealtimePartitionDataReclaimerBase);

RealtimePartitionDataReclaimerBase::RealtimePartitionDataReclaimerBase(const IndexPartitionSchemaPtr& schema)
    : mSchema(schema)
{
}

RealtimePartitionDataReclaimerBase::~RealtimePartitionDataReclaimerBase() {}

void RealtimePartitionDataReclaimerBase::Reclaim(int64_t timestamp, const PartitionDataPtr& buildingPartData)
{
    IE_LOG(INFO, "reclaim begin");
    TrimObsoleteAndEmptyRtSegments(timestamp, buildingPartData);
    RemoveObsoleteRtDocs(timestamp, buildingPartData);
    TrimObsoleteAndEmptyRtSegments(timestamp, buildingPartData);
    IE_LOG(INFO, "reclaim end");
}

void RealtimePartitionDataReclaimerBase::RemoveObsoleteRtDocs(int64_t reclaimTimestamp,
                                                              const PartitionDataPtr& partitionData)
{
    IE_LOG(INFO, "RemoveObsoleteRtDocs begin");
    // TODO: remove args options and calculator
    IndexConfigPtr indexConfig = mSchema->GetIndexSchema()->GetIndexConfig(VIRTUAL_TIMESTAMP_INDEX_NAME);
    std::shared_ptr<InvertedIndexReader> indexReader(
        IndexReaderFactory::CreateIndexReader(indexConfig->GetInvertedIndexType(), /*indexMetrics*/ nullptr));
    assert(indexReader);
    const auto& legacyReader = std::dynamic_pointer_cast<index::LegacyIndexReaderInterface>(indexReader);
    assert(legacyReader);
    legacyReader->Open(indexConfig, partitionData);

    std::shared_ptr<KeyIterator> keyIter = indexReader->CreateKeyIterator("");

    PartitionModifierPtr modifier = CreateModifier(partitionData);
    assert(modifier);

    while (keyIter->HasNext()) {
        string strTs;
        std::shared_ptr<PostingIterator> postIter(keyIter->NextPosting(strTs));
        assert(postIter);

        int64_t ts;
        if (!StringUtil::strToInt64(strTs.c_str(), ts)) {
            continue;
        }
        if (ts >= reclaimTimestamp) {
            break;
        }
        docid_t docId = INVALID_DOCID;
        while ((docId = postIter->SeekDoc(docId)) != INVALID_DOCID) {
            modifier->RemoveDocument(docId);
        }
    }
    if (!modifier->IsDirty()) {
        return;
    }
    DoFinishRemoveObsoleteRtDocs(modifier, partitionData);
    IE_LOG(INFO, "RemoveObsoleteRtDocs end");
}

void RealtimePartitionDataReclaimerBase::TrimObsoleteAndEmptyRtSegments(int64_t reclaimTimestamp,
                                                                        const PartitionDataPtr& partData)
{
    IE_LOG(INFO, "trim begin");
    SimpleSegmentMetas segMetas;
    ExtractRtSegMetasFromPartitionData(partData, segMetas);

    DeletionMapReaderPtr delMapReader = partData->GetDeletionMapReader();
    vector<segmentid_t> segIdsToRemove;
    ExtractSegmentsToReclaim(segMetas, reclaimTimestamp, delMapReader, segIdsToRemove);

    DoFinishTrimObsoleteAndEmptyRtSegments(partData, segIdsToRemove);
    IE_LOG(INFO, "trim end");
}

void RealtimePartitionDataReclaimerBase::TrimBuildingSegment(int64_t reclaimTimestamp, const PartitionDataPtr& partData)
{
    IE_LOG(INFO, "trim building segment begin");

    int64_t buildingTs = INVALID_TIMESTAMP;
    const InMemorySegmentPtr& inMemorySegment = partData->GetInMemorySegment();
    if (inMemorySegment) {
        const SegmentInfoPtr& segmentInfo = inMemorySegment->GetSegmentInfo();
        if (segmentInfo) {
            buildingTs = segmentInfo->timestamp;
        } else {
            assert(false);
            IE_LOG(ERROR, "segmentInfo in inMemorySegment[%s]", inMemorySegment->GetDirectory()->DebugString().c_str());
        }
    }
    DoFinishTrimBuildingSegment(reclaimTimestamp, buildingTs, partData);
    IE_LOG(INFO, "trim building segment end");
}

void RealtimePartitionDataReclaimerBase::ExtractRtSegMetasFromPartitionData(const PartitionDataPtr& partData,
                                                                            SimpleSegmentMetas& segMetas)
{
    segMetas.clear();

    PartitionData::Iterator it;
    for (it = partData->Begin(); it != partData->End(); it++) {
        const SegmentData& segData = *it;
        if (!RealtimeSegmentDirectory::IsRtSegmentId(segData.GetSegmentId())) {
            continue;
        }

        SimpleSegmentMeta meta;
        meta.segmentId = segData.GetSegmentId();
        meta.docCount = segData.GetSegmentInfo()->docCount;
        meta.timestamp = segData.GetSegmentInfo()->timestamp;
        meta.hasOperation = (segData.GetOperationDirectory(false) != NULL);
        segMetas.push_back(meta);
    }
}

void RealtimePartitionDataReclaimerBase::ExtractSegmentsToReclaim(const SimpleSegmentMetas& segMetas,
                                                                  int64_t reclaimTimestamp,
                                                                  const DeletionMapReaderPtr& deletionMap,
                                                                  vector<segmentid_t>& outputSegIds)
{
    outputSegIds.clear();
    if (segMetas.empty()) {
        return;
    }

    for (size_t i = 0; i < segMetas.size(); ++i) {
        if (NeedReclaimSegment(segMetas[i], reclaimTimestamp, deletionMap)) {
            outputSegIds.push_back(segMetas[i].segmentId);
        } else {
            break;
        }
    }
}

bool RealtimePartitionDataReclaimerBase::NeedReclaimSegment(const SimpleSegmentMeta& segMeta, int64_t reclaimTimestamp,
                                                            const DeletionMapReaderPtr& deletionMap)
{
    if (segMeta.timestamp < reclaimTimestamp) {
        return true;
    }

    if (segMeta.hasOperation) {
        return false;
    }

    if (deletionMap && deletionMap->GetDeletedDocCount(segMeta.segmentId) == segMeta.docCount) {
        return true;
    }
    return false;
}
}} // namespace indexlib::partition
