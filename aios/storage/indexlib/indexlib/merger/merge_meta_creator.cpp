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
#include "indexlib/merger/merge_meta_creator.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "alog/Logger.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/normal/attribute/accessor/offline_attribute_segment_reader_container.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/util/segment_directory_base.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/merger/split_strategy/temperature_split_strategy.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergeMetaCreator);

MergeMetaCreator::MergeMetaCreator() {}

MergeMetaCreator::~MergeMetaCreator() {}

SegmentMergeInfos MergeMetaCreator::CreateSegmentMergeInfos(const IndexPartitionSchemaPtr& schema,
                                                            const MergeConfig& mergeConfig,
                                                            const merger::SegmentDirectoryPtr& segDir)
{
    IE_LOG(INFO, "create segmentMergeInfos begin");
    SegmentMergeInfos segMergeInfos;
    bool needSegmentSize = false;
    if (mergeConfig.mergeStrategyStr == REALTIME_MERGE_STRATEGY_STR ||
        mergeConfig.mergeStrategyStr == SHARD_BASED_MERGE_STRATEGY_STR ||
        mergeConfig.mergeStrategyStr == PRIORITY_QUEUE_MERGE_STRATEGY_STR ||
        mergeConfig.mergeStrategyStr == TIMESERIES_MERGE_STRATEGY_STR ||
        mergeConfig.mergeStrategyStr == TEMPERATURE_MERGE_STRATEGY_STR) {
        needSegmentSize = true;
    }
    if (mergeConfig.GetSplitStrategyName() == TemperatureSplitStrategy::STRATEGY_NAME) {
        needSegmentSize = true;
    }
    Version version = segDir->GetVersion();
    const indexlibv2::framework::LevelInfo& levelInfo = version.GetLevelInfo();
    Version::Iterator vIt = version.CreateIterator();
    merger::SegmentDirectory::Iterator it = segDir->CreateIterator();
    exdocid_t baseDocId = 0;
    OnDiskSegmentSizeCalculator segCalculator;

    DeletionMapReaderPtr deletionReader = segDir->GetDeletionMapReader();
    index_base::PartitionDataPtr partData = segDir->GetPartitionData();
    assert(partData);
    while (it.HasNext()) {
        it.Next();
        assert(vIt.HasNext());
        segmentid_t segId = vIt.Next();
        SegmentInfo segInfo = *partData->GetSegmentData(segId).GetSegmentInfo();

        int64_t segSize = 0;
        if (needSegmentSize) {
            segSize = segCalculator.GetSegmentSize(partData->GetSegmentData(segId), schema);
        }

        uint32_t deleteDocCount = deletionReader ? deletionReader->GetDeletedDocCount(segId) : 0;
        uint32_t levelIdx = 0;
        uint32_t inLevelIdx = 0;
        bool ret = levelInfo.FindPosition(segId, levelIdx, inLevelIdx);
        assert(ret);
        (void)ret;

        if (segInfo.shardId == SegmentInfo::INVALID_SHARDING_ID) {
            segInfo.docCount = segInfo.docCount / segInfo.shardCount;
        }

        SegmentMergeInfo segMergeInfo(segId, segInfo, deleteDocCount, baseDocId, segSize, levelIdx, inLevelIdx);
        segMergeInfo.segmentMetrics = *partData->GetSegmentData(segId).GetSegmentMetrics();
        segMergeInfos.push_back(segMergeInfo);

        baseDocId += segInfo.docCount;
    }

    if (schema->EnableTemperatureLayer()) {
        ReWriteSegmentMetric(version, segMergeInfos);
    }

    if (schema->GetTableType() == tt_index && baseDocId >= MAX_DOCID) {
        INDEXLIB_FATAL_ERROR(UnSupported, "partition doc count [%ld] exceed limit [%d]", baseDocId, MAX_DOCID);
    }
    IE_LOG(INFO, "create segmentMergeInfos done");
    return segMergeInfos;
}

void MergeMetaCreator::ReWriteSegmentMetric(const index_base::Version& version,
                                            index_base::SegmentMergeInfos& mergeInfos)
{
    for (auto& mergeInfo : mergeInfos) {
        SegmentTemperatureMeta meta;
        if (!version.GetSegmentTemperatureMeta(mergeInfo.segmentId, meta)) {
            INDEXLIB_FATAL_ERROR(Runtime, "cannot get segment [%d] temperature meta", mergeInfo.segmentId);
        }
        meta.RewriterTemperatureMetric(mergeInfo.segmentMetrics);
    }
}
}} // namespace indexlib::merger
