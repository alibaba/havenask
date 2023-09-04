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
#include "indexlib/table/normal_table/index_task/NormalTableAlterTablePlanCreator.h"

#include "autil/EnvUtil.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskPlan.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/add_index/PatchSegmentMoveOperation.h"
#include "indexlib/table/index_task/merger/EndMergeTaskOperation.h"
#include "indexlib/table/index_task/merger/FenceDirDeleteOperation.h"
#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"
#include "indexlib/table/normal_table/index_task/DropOpLogOperation.h"
#include "indexlib/table/normal_table/index_task/NormalTableAddIndexOperation.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableAlterTablePlanCreator);

const std::string NormalTableAlterTablePlanCreator::TASK_TYPE = framework::ALTER_TABLE_TASK_TYPE;

NormalTableAlterTablePlanCreator::NormalTableAlterTablePlanCreator(const std::string& taskName,
                                                                   const std::map<std::string, std::string>& params)
    : SimpleIndexTaskPlanCreator(taskName, params)
{
}

NormalTableAlterTablePlanCreator::~NormalTableAlterTablePlanCreator() {}

void NormalTableAlterTablePlanCreator::CalculateAlterIndexConfigs(
    const std::shared_ptr<framework::Segment>& segment, schemaid_t baseSchemaId, schemaid_t targetSchemaId,
    const std::shared_ptr<framework::TabletData>& tabletData,
    std::vector<std::shared_ptr<config::IIndexConfig>>& addIndexConfigs,
    std::vector<std::shared_ptr<config::IIndexConfig>>& deleteIndexConfigs) const
{
    auto baseSchema = tabletData->GetTabletSchema(baseSchemaId);
    auto targetSchema = tabletData->GetTabletSchema(targetSchemaId);
    if (segment) {
        baseSchema = segment->GetSegmentSchema();
    }
    auto alterSchemas = tabletData->GetAllTabletSchema(baseSchema->GetSchemaId(), targetSchema->GetSchemaId());
    {
        // get delete index configs
        auto indexConfigs = baseSchema->GetIndexConfigs();
        for (auto indexConfig : indexConfigs) {
            for (auto schema : alterSchemas) {
                if (!schema->GetIndexConfig(indexConfig->GetIndexType(), indexConfig->GetIndexName())) {
                    // any schema has no config, means it has been dropped
                    deleteIndexConfigs.push_back(indexConfig);
                    break;
                }
            }
        }
    }

    {
        // get add index configs
        auto indexConfigs = targetSchema->GetIndexConfigs();
        for (auto indexConfig : indexConfigs) {
            for (auto schema : alterSchemas) {
                if (!schema->GetIndexConfig(indexConfig->GetIndexType(), indexConfig->GetIndexName())) {
                    // any schema has no config, means it has been dropped, so need re add
                    addIndexConfigs.push_back(indexConfig);
                    break;
                }
            }
        }
    }
}

std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
NormalTableAlterTablePlanCreator::CreateTaskPlan(const framework::IndexTaskContext* taskContext)
{
    const auto& version = taskContext->GetTabletData()->GetOnDiskVersion();
    auto baseSchemaId = version.GetReadSchemaId();
    auto targetSchemaId = version.GetSchemaId();
    auto baseSchema = taskContext->GetTabletSchema(baseSchemaId);
    if (!baseSchema) {
        AUTIL_LOG(ERROR, "load base schema [%d] from index root failed", baseSchemaId);
        return std::make_pair(Status::Corruption(), nullptr);
    }
    auto targetSchema = taskContext->GetTabletSchema(targetSchemaId);
    if (!targetSchema) {
        AUTIL_LOG(ERROR, "load target schema [%d] from index root failed", targetSchemaId);
        return std::make_pair(Status::Corruption(), nullptr);
    }

    auto targetVersion = version;
    targetVersion.SetVersionId(taskContext->GetMaxMergedVersionId() + 1);
    targetVersion.SetSchemaId(targetSchemaId);
    targetVersion.SetFenceName("");
    targetVersion.SetReadSchemaId(targetSchemaId);

    auto status = CommitTaskLogToVersion(taskContext, targetVersion);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "commit task log to version failed");

    auto tabletOptions = taskContext->GetTabletOptions();
    auto tabletData = taskContext->GetTabletData();
    if (tabletOptions->GetOnlineConfig().SupportAlterTableWithDefaultValue() &&
        UseDefaultValueStrategy(baseSchemaId, targetSchemaId, tabletData)) {
        AUTIL_LOG(INFO, "Alter normal table [%s] will use default value strategy, schemaId[%d], versionId [%d]",
                  targetSchema->GetTableName().c_str(), targetSchemaId, targetVersion.GetVersionId());
        return CreateDefaultValueTaskPlan(targetVersion);
    }

    framework::IndexOperationId id = 0;
    auto indexTaskPlan = std::make_unique<framework::IndexTaskPlan>(_taskName, TASK_TYPE);
    std::vector<framework::IndexOperationId> segmentMoveOperationIds;
    std::vector<MergedVersionCommitOperation::DropIndexInfo> dropIndexes;
    for (auto [segId, _] : version) {
        std::vector<framework::IndexOperationId> segmentMoveDependIds;
        auto segment = tabletData->GetSegment(segId);
        std::vector<std::shared_ptr<config::IIndexConfig>> addIndexConfigs;
        std::vector<std::shared_ptr<config::IIndexConfig>> deleteIndexConfigs;
        CalculateAlterIndexConfigs(segment, baseSchemaId, targetSchemaId, tabletData, addIndexConfigs,
                                   deleteIndexConfigs);
        for (auto indexConfig : addIndexConfigs) {
            auto desc =
                NormalTableAddIndexOperation::CreateOperationDescription(id, targetSchemaId, segId, indexConfig);
            indexTaskPlan->AddOperation(desc);
            segmentMoveDependIds.push_back(id);
            id++;
        }

        if (DropOpLogOperation::NeedDropOpLog(segId, baseSchema, targetSchema)) {
            auto desc = DropOpLogOperation::CreateOperationDescription(id, segId, targetSchemaId);
            indexTaskPlan->AddOperation(desc);
            segmentMoveDependIds.push_back(id);
            id++;
        }
        if (segmentMoveDependIds.size() == 0) {
            continue;
        }
        auto segDir = version.GetSegmentDirName(segId);
        auto opDesc =
            PatchSegmentMoveOperation::CreateOperationDescription(id, segDir, targetSchemaId, segmentMoveDependIds);
        opDesc.AddDepends(segmentMoveDependIds);
        indexTaskPlan->AddOperation(opDesc);
        segmentMoveOperationIds.push_back(id);
        id++;
    }

    // prepare version commit operation
    for (auto [segmentId, _] : targetVersion) {
        targetVersion.UpdateSegmentSchemaId(segmentId, targetSchemaId);
    }

    std::string patchIndexDir;
    if (segmentMoveOperationIds.size() > 0) {
        patchIndexDir = PathUtil::GetPatchIndexDirName(targetSchemaId);
    }
    auto commitOpDesc =
        MergedVersionCommitOperation::CreateOperationDescription(id++, targetVersion, patchIndexDir, dropIndexes);
    commitOpDesc.AddDepends(segmentMoveOperationIds);
    indexTaskPlan->AddOperation(commitOpDesc);

    auto delFenceOp = FenceDirDeleteOperation::CreateOperationDescription(id++);
    delFenceOp.AddDepend(commitOpDesc.GetId());
    indexTaskPlan->AddOperation(delFenceOp);

    indexTaskPlan->SetEndTaskOperation(EndMergeTaskOperation::CreateOperationDescription(id++, targetVersion));
    return std::make_pair(Status::OK(), std::move(indexTaskPlan));
}

bool NormalTableAlterTablePlanCreator::UseDefaultValueStrategy(
    schemaid_t baseSchemaId, schemaid_t targetSchemaId, const std::shared_ptr<framework::TabletData>& tabletData) const
{
    std::vector<std::shared_ptr<config::IIndexConfig>> addIndexConfigs;
    std::vector<std::shared_ptr<config::IIndexConfig>> deleteIndexConfigs;
    CalculateAlterIndexConfigs(nullptr, baseSchemaId, targetSchemaId, tabletData, addIndexConfigs, deleteIndexConfigs);
    if (deleteIndexConfigs.size() > 0) {
        AUTIL_LOG(INFO, "new schema has drop index, will complete index in task");
        return false;
    }
    auto baseSchema = tabletData->GetTabletSchema(baseSchemaId);
    for (auto& config : addIndexConfigs) {
        auto fieldConfigs = config->GetFieldConfigs();
        for (auto& fieldConfig : fieldConfigs) {
            if (baseSchema->GetFieldConfig(fieldConfig->GetFieldName()) != nullptr) {
                // field already exist in base schema, maybe add index from current field
                AUTIL_LOG(INFO, "field [%s] already exist in base schema, will complete index in task",
                          fieldConfig->GetFieldName().c_str());
                return false;
            }
            if (!fieldConfig->GetUserDefinedParam().empty()) {
                // has udf param maybe depends on other fields
                AUTIL_LOG(INFO, "field [%s] use udf param to set, will complete index in task",
                          fieldConfig->GetFieldName().c_str());
                return false;
            }
            if (fieldConfig->IsEnableNullField()) {
                AUTIL_LOG(INFO, "field [%s] support null, will complete index in task",
                          fieldConfig->GetFieldName().c_str());
                return false;
            }
            auto attrConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(config);
            if (attrConfig && attrConfig->IsAttributeUpdatable()) {
                // updateable attribute not support
                AUTIL_LOG(INFO, "field [%s] is updateable attribute, will complete index in task",
                          fieldConfig->GetFieldName().c_str());
                return false;
            }
            auto indexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(config);
            if (indexConfig && UnsupportIndexType(indexConfig)) {
                return false;
            }
        }
    }
    return true;
}

bool NormalTableAlterTablePlanCreator::UnsupportIndexType(
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig) const
{
    if (indexConfig->GetInvertedIndexType() == it_text || indexConfig->GetInvertedIndexType() == it_range ||
        indexConfig->GetInvertedIndexType() == it_date || indexConfig->GetInvertedIndexType() == it_expack ||
        indexConfig->GetInvertedIndexType() == it_expack) {
        // not support type
        AUTIL_LOG(INFO, "field [%s] is complicated index type[%d], will complete index in task",
                  indexConfig->GetIndexName().c_str(), indexConfig->GetInvertedIndexType());
        return true;
    }
    optionflag_t option = indexConfig->GetOptionFlag();
    if ((option & of_term_frequency) | (option & of_doc_payload) | (option & of_position_payload) |
        (option & of_tf_bitmap) | (option & of_fieldmap)) {
        AUTIL_LOG(INFO, "field [%s] is complicated index option [%d], will complete index in task",
                  indexConfig->GetIndexName().c_str(), option);
        return true;
    }
    if (indexConfig->GetHighFreqVocabulary() != nullptr) {
        // not support yet
        AUTIL_LOG(INFO, "field [%s] set for high freq, will complete index in task",
                  indexConfig->GetIndexName().c_str());
        return true;
    }
    if (indexConfig->GetShardingType() == indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING) {
        AUTIL_LOG(INFO, "field [%s] is set for multi shard, will complete index in task",
                  indexConfig->GetIndexName().c_str());
        return true;
    }
    if (indexConfig->IsIndexUpdatable()) {
        // not support yet
        AUTIL_LOG(INFO, "field [%s] set updatable, will complete index in task", indexConfig->GetIndexName().c_str());
        return true;
    }
    return false;
}

std::pair<Status, std::unique_ptr<framework::IndexTaskPlan>>
NormalTableAlterTablePlanCreator::CreateDefaultValueTaskPlan(const framework::Version& targetVersion)
{
    auto indexTaskPlan = std::make_unique<framework::IndexTaskPlan>(_taskName, TASK_TYPE);
    framework::IndexOperationId id = 0;
    auto commitOpDesc =
        MergedVersionCommitOperation::CreateOperationDescription(id++, targetVersion, /*patchIndexDir*/ "", {});
    indexTaskPlan->AddOperation(commitOpDesc);

    auto delFenceOp = FenceDirDeleteOperation::CreateOperationDescription(id++);
    delFenceOp.AddDepend(commitOpDesc.GetId());
    indexTaskPlan->AddOperation(delFenceOp);

    indexTaskPlan->SetEndTaskOperation(EndMergeTaskOperation::CreateOperationDescription(id, targetVersion));
    return std::make_pair(Status::OK(), std::move(indexTaskPlan));
}

std::string NormalTableAlterTablePlanCreator::ConstructLogTaskType() const
{
    std::string logType = TASK_TYPE;
    if (!_taskName.empty()) {
        logType += "@";
        logType += _taskName;
    }
    return logType;
}

std::string NormalTableAlterTablePlanCreator::ConstructLogTaskId(const framework::IndexTaskContext* taskContext,
                                                                 const framework::Version& targetVersion) const
{
    const auto& baseVersion = taskContext->GetTabletData()->GetOnDiskVersion();
    std::string taskId = "schema_";
    taskId += autil::StringUtil::toString(baseVersion.GetReadSchemaId());
    taskId += "_to_";
    taskId += autil::StringUtil::toString(targetVersion.GetSchemaId());
    return taskId;
}

} // namespace indexlibv2::table
