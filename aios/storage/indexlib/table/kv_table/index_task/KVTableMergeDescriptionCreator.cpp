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
#include "indexlib/table/kv_table/index_task/KVTableMergeDescriptionCreator.h"

#include "indexlib/index/kv/Common.h"

using namespace std;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableMergeDescriptionCreator);

KVTableMergeDescriptionCreator::KVTableMergeDescriptionCreator(const std::shared_ptr<config::TabletSchema>& schema)
    : CommonMergeDescriptionCreator(schema)
{
}

KVTableMergeDescriptionCreator::~KVTableMergeDescriptionCreator() {}

std::pair<Status, framework::IndexOperationDescription>
KVTableMergeDescriptionCreator::CreateIndexMergeOperationDescription(
    const std::shared_ptr<MergePlan>& mergePlan, const std::shared_ptr<config::IIndexConfig>& indexConfig,
    framework::IndexOperationId operationId, size_t planIdx)
{
    auto [status, opDesc] = CommonMergeDescriptionCreator::CreateIndexMergeOperationDescription(mergePlan, indexConfig,
                                                                                                operationId, planIdx);
    if (!status.IsOK()) {
        return std::make_pair(status, opDesc);
    }
    auto segMergePlan = mergePlan->GetSegmentMergePlan(planIdx);
    assert(segMergePlan.GetTargetSegmentCount() == 1);
    auto targetSegmentId = segMergePlan.GetTargetSegmentId(0);

    const auto& targetVersion = mergePlan->GetTargetVersion();
    auto segDesc = targetVersion.GetSegmentDescriptions();
    auto levelInfo = segDesc->GetLevelInfo();
    if (!levelInfo) {
        assert(levelInfo);
        AUTIL_LOG(ERROR, "no level info");
        return std::make_pair(Status::Corruption(), opDesc);
    }
    uint32_t levelIdx;
    uint32_t inLevelIdx;
    size_t levelCount = levelInfo->GetLevelCount();
    if (!levelInfo->FindPosition(targetSegmentId, levelIdx, inLevelIdx)) {
        assert(false);
        AUTIL_LOG(ERROR, "not found segment [%d] levelIdx in level info", targetSegmentId);
        return std::make_pair(Status::Corruption(), opDesc);
    }
    if (levelCount == levelIdx + 1) {
        opDesc.AddParameter(index::DROP_DELETE_KEY, "true");
    } else {
        opDesc.AddParameter(index::DROP_DELETE_KEY, "false");
    }
    if (_currentTimestamp == -1) {
        _currentTimestamp = autil::TimeUtility::currentTimeInSeconds();
    }
    opDesc.AddParameter(index::CURRENT_TIME_IN_SECOND, autil::StringUtil::toString(_currentTimestamp));
    return std::make_pair(Status::OK(), opDesc);
}

} // namespace indexlibv2::table
