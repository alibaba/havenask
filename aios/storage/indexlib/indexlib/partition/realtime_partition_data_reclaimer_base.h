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
#ifndef __INDEXLIB_REALTIME_PARTITION_DATA_RECLAIMER_BASE_H
#define __INDEXLIB_REALTIME_PARTITION_DATA_RECLAIMER_BASE_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/modifier/partition_modifier.h"
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace partition {

class RealtimePartitionDataReclaimerBase
{
private:
    struct SimpleSegmentMeta {
        segmentid_t segmentId;
        uint32_t docCount;
        int64_t timestamp;
        bool hasOperation;
    };

    typedef std::vector<SimpleSegmentMeta> SimpleSegmentMetas;

public:
    RealtimePartitionDataReclaimerBase(const config::IndexPartitionSchemaPtr& schema);
    virtual ~RealtimePartitionDataReclaimerBase();

public:
    void Reclaim(int64_t timestamp, const index_base::PartitionDataPtr& buildingPartData);

    void TrimObsoleteAndEmptyRtSegments(int64_t reclaimTimestamp, const index_base::PartitionDataPtr& partitionData);

    void TrimBuildingSegment(int64_t reclaimTimestamp, const index_base::PartitionDataPtr& partData);

private:
    void RemoveObsoleteRtDocs(int64_t reclaimTimestamp, const index_base::PartitionDataPtr& partitionData);

    void ExtractRtSegMetasFromPartitionData(const index_base::PartitionDataPtr& partData, SimpleSegmentMetas& segMetas);

    void ExtractSegmentsToReclaim(const SimpleSegmentMetas& segMetas, int64_t reclaimTimestamp,
                                  const index::DeletionMapReaderPtr& deletionMap,
                                  std::vector<segmentid_t>& outputSegIds);

    bool NeedReclaimSegment(const SimpleSegmentMeta& segMeta, int64_t timestamp,
                            const index::DeletionMapReaderPtr& deletionMap);

protected:
    virtual void DoFinishTrimBuildingSegment(int64_t reclaimTimestamp, int64_t buildingTs,
                                             const index_base::PartitionDataPtr& partData) = 0;
    virtual void DoFinishRemoveObsoleteRtDocs(PartitionModifierPtr modifier,
                                              const index_base::PartitionDataPtr& partData) = 0;

    virtual void DoFinishTrimObsoleteAndEmptyRtSegments(const index_base::PartitionDataPtr& partData,
                                                        const std::vector<segmentid_t>& segIdsToRemove) = 0;

    virtual PartitionModifierPtr CreateModifier(const index_base::PartitionDataPtr& partData) = 0;

protected:
    config::IndexPartitionSchemaPtr mSchema;

private:
    friend class RealtimePartitionDataReclaimerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RealtimePartitionDataReclaimerBase);
}} // namespace indexlib::partition

#endif //__INDEXLIB_REALTIME_PARTITION_DATA_RECLAIMER_BASE_H
