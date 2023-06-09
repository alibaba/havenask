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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/MetricsWrapper.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"

namespace indexlibv2::framework {
struct SegmentMeta;
}

namespace indexlibv2::index {
class DeletionMapPatchWriter;
}
namespace indexlibv2::table {
class IndexReclaimerParam;

class NormalTableReclaimOperation : public framework::IndexOperation
{
public:
    NormalTableReclaimOperation(const framework::IndexOperationDescription& desc);
    ~NormalTableReclaimOperation() = default;

public:
    static framework::IndexOperationDescription
    CreateOperationDescription(indexlibv2::framework::IndexOperationId id, const std::string& segDirName,
                               segmentid_t targetSegmentId, const std::vector<segmentid_t>& removedSegments);
    Status Execute(const framework::IndexTaskContext& context) override;

    static constexpr char OPERATION_TYPE[] = "normal_table_reclaim_operation";
    static constexpr char REMOVED_SEGMENTS[] = "removed_segments";
    static constexpr char TARGET_SEGMENT_DIR[] = "target_segment_dir";
    static constexpr char TARGET_SEGMENT_ID[] = "target_segment_id";

private:
    std::pair<Status, std::vector<IndexReclaimerParam>>
    LoadIndexReclaimerParams(const framework::IndexTaskContext& context);

    std::pair<Status, framework::SegmentMeta> CreateSegmentMeta(const framework::IndexTaskContext& context,
                                                                const std::string& targetSegmentDirName,
                                                                segmentid_t targetSegmentId,
                                                                segmentid_t maxMergedSegmentId);

    Status DumpSegment(const framework::IndexTaskContext& context, const framework::SegmentMeta& segmentMeta,
                       const index::DeletionMapPatchWriter& deletionMapPatchWriter);

    Status ReclaimByTTL(const framework::IndexTaskContext& context,
                        const std::map<segmentid_t, size_t>& segmentId2DocCount, index::DeletionMapPatchWriter* writer);

private:
    framework::IndexOperationDescription _desc;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    INDEXLIB_FM_DECLARE_METRIC(TTLReclaim);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
