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
#include "indexlib/table/index_task/merger/MultiShardIndexMergeOperation.h"

#include "autil/StringUtil.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"

using namespace std;

namespace indexlibv2 { namespace table {
AUTIL_LOG_SETUP(indexlib.table, MultiShardIndexMergeOperation);

MultiShardIndexMergeOperation::MultiShardIndexMergeOperation(const framework::IndexOperationDescription& desc)
    : IndexMergeOperation(desc)
{
}

MultiShardIndexMergeOperation::~MultiShardIndexMergeOperation() {}

std::pair<Status, index::IIndexMerger::SegmentMergeInfos>
MultiShardIndexMergeOperation::PrepareSegmentMergeInfos(const framework::IndexTaskContext& context,
                                                        bool prepareTargetSegDir)
{
    auto tabletData = context.GetTabletData();
    SegmentMergePlan segMergePlan;
    framework::Version targetVersion;
    auto status = GetSegmentMergePlan(context, segMergePlan, targetVersion);
    if (!status.IsOK()) {
        return std::make_pair(status, index::IIndexMerger::SegmentMergeInfos());
    }
    auto targetSegmentId = segMergePlan.GetTargetSegmentId(0);
    auto segDescriptions = targetVersion.GetSegmentDescriptions();
    auto levelInfo = segDescriptions->GetLevelInfo();
    assert(levelInfo);
    uint32_t levelIdx;
    uint32_t inLevelIdx;
    if (!levelInfo->FindPosition(targetSegmentId, levelIdx, inLevelIdx)) {
        assert(false);
        std::string levelInfoStr = indexlib::file_system::JsonUtil::ToString(*levelInfo).Value();
        AUTIL_LOG(ERROR, "can not find position for target segment[%d], levelInfo: %s", targetSegmentId,
                  levelInfoStr.c_str());
        return std::make_pair(Status::Corruption("can not find postion for target segment"),
                              index::IIndexMerger::SegmentMergeInfos());
    }

    index::IIndexMerger::SegmentMergeInfos segMergeInfos;
    for (size_t i = 0; i < segMergePlan.GetSrcSegmentCount(); i++) {
        index::IIndexMerger::SourceSegment sourceSegment;
        std::tie(sourceSegment.segment, sourceSegment.baseDocid) =
            tabletData->GetSegmentWithBaseDocid(segMergePlan.GetSrcSegmentId(i));
        auto multiShardSegment = dynamic_pointer_cast<plain::MultiShardDiskSegment>(sourceSegment.segment);
        sourceSegment.segment = multiShardSegment->GetShardSegment(inLevelIdx);
        assert(sourceSegment.segment);
        segMergeInfos.srcSegments.push_back(sourceSegment);
    }
    std::shared_ptr<framework::SegmentMeta> segMeta(new framework::SegmentMeta(targetSegmentId));
    const auto& segInfo = segMergePlan.GetTargetSegmentInfo(0);
    segMeta->segmentInfo.reset(new framework::SegmentInfo(segInfo));
    segMeta->schema = context.GetTabletSchema();
    if (prepareTargetSegDir) {
        auto [status, fenceDir] = context.GetOpFenceRoot(_desc.GetId(), _desc.UseOpFenceDir());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "get op fence dir failed");
            return std::make_pair(status, index::IIndexMerger::SegmentMergeInfos());
        }
        auto indexPaths = _indexConfig->GetIndexPath();
        auto [status1, segDir] =
            PrepareTargetSegDir(fenceDir, targetVersion.GetSegmentDirName(targetSegmentId), indexPaths);
        if (!status1.IsOK()) {
            AUTIL_LOG(ERROR, "prepare target segment dir failed");
            return std::make_pair(status1, index::IIndexMerger::SegmentMergeInfos());
        }
        segMeta->segmentDir = segDir;
    }

    segMergeInfos.targetSegments.push_back(segMeta);
    return std::make_pair(Status::OK(), segMergeInfos);
}

}} // namespace indexlibv2::table
