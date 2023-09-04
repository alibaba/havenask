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
#include "indexlib/table/index_task/merger/CommonMergeDescriptionCreator.h"

#include "autil/StringUtil.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/file_system/JsonUtil.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/FenceDirDeleteOperation.h"
#include "indexlib/table/index_task/merger/IndexMergeOperation.h"
#include "indexlib/table/index_task/merger/MergedSegmentMoveOperation.h"
#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, CommonMergeDescriptionCreator);

CommonMergeDescriptionCreator::CommonMergeDescriptionCreator(const std::shared_ptr<config::ITabletSchema>& schema)
    : _schema(schema)
{
}

CommonMergeDescriptionCreator::~CommonMergeDescriptionCreator() {}

std::pair<Status, framework::IndexOperationDescription>
CommonMergeDescriptionCreator::CreateIndexMergeOperationDescription(
    const std::shared_ptr<MergePlan>& mergePlan, const std::shared_ptr<config::IIndexConfig>& indexConfig,
    framework::IndexOperationId operationId, size_t planIdx)
{
    const string& indexName = indexConfig->GetIndexName();
    const string& indexType = indexConfig->GetIndexType();

    framework::IndexOperationDescription opDesc(operationId, IndexMergeOperation::OPERATION_TYPE);
    opDesc.AddParameter(MERGE_INDEX_NAME, indexName);
    opDesc.AddParameter(MERGE_INDEX_TYPE, indexType);
    opDesc.AddParameter(MERGE_PLAN_INDEX, std::to_string(planIdx));
    return std::make_pair(Status::OK(), opDesc);
}

std::vector<std::shared_ptr<config::IIndexConfig>> CommonMergeDescriptionCreator::GetSupportConfigs()
{
    return _schema->GetIndexConfigs();
}

std::pair<Status, std::vector<framework::IndexOperationDescription>>
CommonMergeDescriptionCreator::CreateMergeOperationDescriptions(const std::shared_ptr<MergePlan>& mergePlan)
{
    std::vector<framework::IndexOperationDescription> operationDescs;
    std::vector<framework::IndexOperationDescription> emptyDescs;
    const auto& version = mergePlan->GetTargetVersion();
    const auto& indexConfigs = GetSupportConfigs();
    framework::IndexOperationId operationId = 0;
    std::vector<framework::IndexOperationId> segmentOperationIds;

    auto initMergeOpDesc = CreateInitMergeOperationDescription(mergePlan, operationId);
    if (initMergeOpDesc) {
        operationId++;
        operationDescs.push_back(*initMergeOpDesc);
    }

    for (size_t planIdx = 0; planIdx < mergePlan->Size(); ++planIdx) {
        std::map<framework::IndexOperationId, std::pair<std::string, std::string>> opToIndex;
        std::vector<framework::IndexOperationId> segmentMoveDependIds;

        auto [beforeIndexMergeOpId, beforeIndexMergeOpDescs] =
            CreateBeforeMergeIndexOperationDescriptions(operationId, initMergeOpDesc.get(), planIdx);
        for (const auto& desc : beforeIndexMergeOpDescs) {
            operationId++;
            operationDescs.push_back(*desc);
        }

        for (const auto& indexConfig : indexConfigs) {
            auto [status, opDesc] = CreateIndexMergeOperationDescription(mergePlan, indexConfig, operationId, planIdx);
            if (!status.IsOK()) {
                return std::make_pair(status, emptyDescs);
            }
            if (beforeIndexMergeOpId != framework::INVALID_INDEX_OPERATION_ID) {
                opDesc.AddDepend(beforeIndexMergeOpId);
            }
            if (initMergeOpDesc) {
                opDesc.AddDepend(initMergeOpDesc->GetId());
                opDesc.AddParameter(DEPENDENT_OPERATION_ID, autil::StringUtil::toString(initMergeOpDesc->GetId()));
            }
            opToIndex[operationId] = std::make_pair(indexConfig->GetIndexType(), indexConfig->GetIndexName());
            operationDescs.push_back(opDesc);
            segmentMoveDependIds.push_back(operationId);
            operationId++;
        }
        auto& segMergePlan = mergePlan->GetSegmentMergePlan(planIdx);
        for (size_t i = 0; i < segMergePlan.GetTargetSegmentCount(); i++) {
            auto segId = segMergePlan.GetTargetSegmentId(i);
            auto segDir = version.GetSegmentDirName(segId);
            auto segInfo = segMergePlan.GetTargetSegmentInfo(i);
            framework::IndexOperationDescription opDesc(operationId, MergedSegmentMoveOperation::OPERATION_TYPE);
            opDesc.AddParameter(MergedSegmentMoveOperation::PARAM_TARGET_SEGMENT_DIR, segDir);
            opDesc.AddParameter(MergedSegmentMoveOperation::PARAM_TARGET_SEGMENT_INFO,
                                autil::legacy::ToJsonString(segInfo));
            opDesc.AddParameter(MergedSegmentMoveOperation::PARAM_OP_TO_INDEX, autil::legacy::ToJsonString(opToIndex));
            opDesc.AddDepends(segmentMoveDependIds);
            operationDescs.push_back(opDesc);
            segmentOperationIds.push_back(operationId);
            operationId++;
        }
    }
    auto commitOpDesc =
        MergedVersionCommitOperation::CreateOperationDescription(operationId++, version, /*patchDir*/ "",
                                                                 /*dropIndexes*/ {});
    commitOpDesc.AddDepends(segmentOperationIds);
    operationDescs.push_back(commitOpDesc);

    auto delFenceOpDesc = FenceDirDeleteOperation::CreateOperationDescription(operationId);
    delFenceOpDesc.AddDepend(commitOpDesc.GetId());

    operationDescs.push_back(delFenceOpDesc);
    return std::make_pair(Status::OK(), operationDescs);
}

} // namespace indexlibv2::table
