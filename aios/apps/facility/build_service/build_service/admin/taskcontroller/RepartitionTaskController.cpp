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
#include "build_service/admin/taskcontroller/RepartitionTaskController.h"

#include <memory>
#include <ostream>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointFormatter.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/ErrorLogCollector.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace build_service::util;

using build_service::common::CheckpointAccessorPtr;
using build_service::common::IndexCheckpointFormatter;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, RepartitionTaskController);

bool RepartitionTaskParam::check(string& errorMsg)
{
    if (generationId < 0) {
        errorMsg = "not define generation id for target index";
        return false;
    }

    if (partitionCount <= 0) {
        errorMsg = "invalid partition count for target index";
        return false;
    }

    if (parallelNum <= 0) {
        errorMsg = "invalid parallel num for target index";
        return false;
    }

    if (indexPath.empty()) {
        errorMsg = "not define index path for target index";
        return false;
    }
    return true;
}

RepartitionTaskController::~RepartitionTaskController() {}

RepartitionTaskController::RepartitionTaskController(const RepartitionTaskController& other)
    : DefaultTaskController(other)
{
}

bool RepartitionTaskController::validateConfig(const std::string& clusterName)
{
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    config::ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();
    auto schema = resourceReader->getSchema(clusterName);
    if (schema->GetTableType() != tt_index) {
        BS_LOG(ERROR, "repartition task only support normal index table");
        return false;
    }
    auto indexSchema = schema->GetIndexSchema();
    if (!checkIndexSchema(indexSchema)) {
        return false;
    }
    auto subSchema = schema->GetSubIndexPartitionSchema();
    if (!subSchema) {
        return true;
    }
    auto subIndexSchema = subSchema->GetIndexSchema();
    if (!checkIndexSchema(subIndexSchema)) {
        return false;
    }
    return true;
}

bool RepartitionTaskController::checkIndexSchema(const indexlib::config::IndexSchemaPtr& indexSchema)
{
    auto iter = indexSchema->Begin();
    for (; iter != indexSchema->End(); iter++) {
        auto indexConfig = *iter;
        if (indexConfig->GetInvertedIndexType() == it_customized) {
            BS_LOG(ERROR, "repartition task not support custom index");
            return false;
        }
        if (indexConfig->HasAdaptiveDictionary() &&
            indexConfig->GetHighFrequencyTermPostingType() == indexlib::index::hp_bitmap) {
            BS_LOG(ERROR, "repartition task not support bitmap only index");
            return false;
        }
    }
    return true;
}

bool RepartitionTaskController::prepareIndexPath(const RepartitionTaskParam& param, const string& taskName,
                                                 const string& clusterName)
{
    string targetGenerationPath =
        IndexPathConstructor::getGenerationIndexPath(param.indexPath, clusterName, param.generationId);
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(targetGenerationPath, exist)) {
        BS_LOG(ERROR, "fail to judge generation path [%s] exist", targetGenerationPath.c_str());
        return false;
    }

    if (exist) {
        BS_LOG(ERROR, "generation path [%s] already exist", targetGenerationPath.c_str());
        return false;
    }

    if (!fslib::util::FileUtil::mkDir(targetGenerationPath, true)) {
        BS_LOG(ERROR, "prepare generation path [%s] failed", targetGenerationPath.c_str());
        return false;
    }

    return true;
}

void RepartitionTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    DefaultTaskController::Jsonize(json);
    json.Jsonize("processor_checkpoint", _processorCheckpoint, _processorCheckpoint);
}

bool RepartitionTaskController::doInit(const std::string& clusterName, const std::string& taskConfigPath,
                                       const std::string& initParam)
{
    CheckpointAccessorPtr checkPointAccessor;
    _resourceManager->getResource(checkPointAccessor);
    string checkpointId = IndexCheckpointFormatter::getIndexCheckpointId(true, clusterName);
    string checkPointName;
    string checkPointValue;
    if (!checkPointAccessor->getLatestCheckpoint(checkpointId, checkPointName, checkPointValue)) {
        BS_LOG(ERROR, "get checkpoint failed for task [%s], cluster is [%s]", _taskId.c_str(), clusterName.c_str());
        return false;
    }
    string errorMsg;
    if (!checkPointAccessor->createSavepoint(_taskId, checkpointId, checkPointName, errorMsg)) {
        BS_LOG(ERROR, "create savepoint failed for task [%s], error is [%s]", _taskId.c_str(), errorMsg.c_str());
        return false;
    }

    if (!validateConfig(clusterName)) {
        return false;
    }

    RepartitionTaskParam param;
    try {
        FromJsonString(param, initParam);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "invalid repartition task param:" << initParam;
        BS_LOG(ERROR, "%s", ss.str().c_str());
        return false;
    }

    if (!param.check(errorMsg)) {
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _partitionCount = param.partitionCount;
    _parallelNum = param.parallelNum;

    if (!prepareIndexPath(param, _taskName, clusterName)) {
        return false;
    }
    proto::CheckpointInfo checkpoint;
    IndexCheckpointFormatter::decodeCheckpoint(checkPointValue, checkpoint);
    _processorCheckpoint = autil::StringUtil::toString(checkpoint.processorcheckpoint());
    _targets.clear();
    int64_t curTs = TimeUtility::currentTimeInSeconds();
    string timestampStr = StringUtil::toString(curTs);
    config::TaskTarget beginTarget("begin_task");
    beginTarget.addTargetDescription(config::BS_SNAPSHOT_VERSION, checkPointName);
    beginTarget.addTargetDescription("timestamp", timestampStr);
    beginTarget.addTargetDescription("indexPath", param.indexPath);
    beginTarget.addTargetDescription("generationId", StringUtil::toString(param.generationId));
    beginTarget.addTargetDescription("parallelNum", StringUtil::toString(_parallelNum));
    beginTarget.addTaskConfigPath(taskConfigPath);
    beginTarget.setPartitionCount(_partitionCount);
    beginTarget.setParallelNum(1);
    _targets.push_back(beginTarget);

    config::TaskTarget doTarget("do_task");
    doTarget.addTargetDescription(config::BS_SNAPSHOT_VERSION, checkPointName);
    doTarget.addTaskConfigPath(taskConfigPath);
    doTarget.addTargetDescription("timestamp", timestampStr);
    doTarget.addTargetDescription("indexPath", param.indexPath);
    doTarget.addTargetDescription("generationId", StringUtil::toString(param.generationId));
    doTarget.addTargetDescription("parallelNum", StringUtil::toString(_parallelNum));
    doTarget.setPartitionCount(_partitionCount);
    doTarget.setParallelNum(_parallelNum);
    _targets.push_back(doTarget);

    config::TaskTarget endTarget("end_task");
    endTarget.addTargetDescription(config::BS_SNAPSHOT_VERSION, checkPointName);
    endTarget.addTaskConfigPath(taskConfigPath);
    endTarget.addTargetDescription("indexPath", param.indexPath);
    endTarget.addTargetDescription("generationId", StringUtil::toString(param.generationId));
    endTarget.addTargetDescription("parallelNum", StringUtil::toString(_parallelNum));
    endTarget.addTargetDescription("timestamp", timestampStr);
    endTarget.setPartitionCount(_partitionCount);
    endTarget.setParallelNum(1);
    _targets.push_back(endTarget);

    return true;
}
}} // namespace build_service::admin
