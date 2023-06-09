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
#include "build_service/task_base/BuildTask.h"

#include "autil/legacy/jsonizable.h"
#include "build_service/builder/LineDataBuilder.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/ResourceReaderManager.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/table/BuiltinDefine.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::proto;
using namespace build_service::workflow;
using namespace build_service::builder;
using namespace build_service::config;
using namespace build_service::util;
using namespace indexlib;

using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::config;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, BuildTask);

BuildTask::BuildTask(const std::string& epochId) : TaskBase(epochId) {}

BuildTask::~BuildTask() {}

bool BuildTask::run(const string& jobParam, FlowFactory* brokerFactory, int32_t instanceId, Mode mode, bool isTablet)
{
    if (!TaskBase::init(jobParam)) {
        BS_LOG(ERROR, "TaskBase init failed");
        return false;
    }
    PartitionId partitionId = createPartitionId(instanceId, mode, "builder");
    if (!resetResourceReader(partitionId)) {
        BS_LOG(ERROR, "reset resource reader failed");
        return false;
    }
    if (!startMonitor(partitionId.range(), mode)) {
        string errorMsg = "start monitor failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    string clusterName = getValueFromKeyValueMap(_kvMap, CLUSTER_NAMES);
    std::string tableType;
    if (!_resourceReader->getTableType(clusterName, tableType)) {
        BS_LOG(ERROR, "get table type for cluster[%s] failed", clusterName.c_str());
        return false;
    }
    if (indexlib::table::TABLE_TYPE_RAWFILE == tableType) {
        // PROCESSOR
        if (BUILD_MAP == mode) {
            return true;
        } else {
            return buildRawFileIndex(_resourceReader, _kvMap, partitionId);
        }
    } else {
        return startBuildFlow(partitionId, brokerFactory, mode, isTablet);
    }
}

bool BuildTask::startBuildFlow(const PartitionId& partitionId, FlowFactory* brokerFactory, Mode mode, bool isTablet)
{
    unique_ptr<BuildFlow> buildFlow(createBuildFlow(isTablet));
    BuildFlowMode buildFlowMode = getBuildFlowMode(_jobConfig.needPartition(), mode);
    if (buildFlowMode == BuildFlowMode::NONE) {
        return true;
    }
    _kvMap[BUILDER_BRANCH_NAME_LEGACY] = "true";
    if (!buildFlow->startBuildFlow(_resourceReader, partitionId, _kvMap, brokerFactory, buildFlowMode, JOB,
                                   _metricProvider)) {
        BS_LOG(ERROR, "start buildflow failed");
        return false;
    }

    // BuildFlow will exit itself when JOB mode
    if (!buildFlow->waitFinish()) {
        BS_LOG(ERROR, "fatal error happened!");
        return false;
    }
    BS_LOG(INFO, "build is finished");

    builder::Builder* builder = buildFlow->getBuilder();
    if (_jobConfig.getBuildMode() == BUILD_MODE_INCREMENTAL && builder &&
        !builder->merge(_mergeIndexPartitionOptions)) {
        string errorMsg = "merge failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    builder::BuilderV2* builderV2 = buildFlow->getBuilderV2();
    if (_jobConfig.getBuildMode() == BUILD_MODE_INCREMENTAL && builderV2) {
        if (!builderV2->merge()) {
            BS_LOG(ERROR, "tablet merge failed.");
            return false;
        }
    }

    return true;
}

BuildFlow* BuildTask::createBuildFlow(bool isTablet) const
{
    if (isTablet) {
        std::shared_ptr<indexlibv2::config::TabletSchema> tabletSchema;
        return new BuildFlow(nullptr, tabletSchema, workflow::BuildFlowThreadResource());
    }
    return new BuildFlow();
}

BuildFlowMode BuildTask::getBuildFlowMode(bool needPartition, Mode mode)
{
    if (!needPartition) {
        return mode == BUILD_MAP ? BuildFlowMode::ALL : BuildFlowMode::NONE;
    } else {
        return mode == BUILD_MAP ? BuildFlowMode::PROCESSOR : BuildFlowMode::BUILDER;
    }
}

bool BuildTask::resetResourceReader(const PartitionId& partitionId)
{
    string indexRootPath = getValueFromKeyValueMap(_kvMap, INDEX_ROOT_PATH);
    if (indexRootPath.empty()) {
        string errorMsg = "index root path can not be empty";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    string indexPath = IndexPathConstructor::constructIndexPath(indexRootPath, partitionId);
    _resourceReader.reset(new ResourceReader(_resourceReader->getConfigPath()));
    _resourceReader->init();
    if (!_resourceReader->initGenerationMeta(indexPath)) {
        string errorMsg = "ResourceReader init failed";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

// TODO: ut
bool BuildTask::buildRawFileIndex(const ResourceReaderPtr& resourceReader, const KeyValueMap& kvMap,
                                  const PartitionId& partitionId)
{
    string src = getValueFromKeyValueMap(kvMap, READ_SRC);
    if (src != FILE_READ_SRC) {
        BS_LOG(ERROR, "expect src [file], actual [%s]", src.c_str());
        return false;
    }
    string file = getValueFromKeyValueMap(kvMap, DATA_PATH);
    string indexRoot = getValueFromKeyValueMap(kvMap, INDEX_ROOT_PATH);
    string indexPath = IndexPathConstructor::constructIndexPath(indexRoot, partitionId);
    string fileFolder, fileName;
    fslib::util::FileUtil::splitFileName(file, fileFolder, fileName); // TODO: FileSystem

    auto schema = resourceReader->getSchema(partitionId.clusternames(0));
    if (!schema) {
        return false;
    }
    auto& param = schema->GetUserDefinedParam();
    param[BS_FILE_NAME] = autil::legacy::Any(fileName);

    fslib::ErrorCode ec = FileSystem::mkDir(indexPath, true);
    if (ec != EC_OK && ec != EC_EXIST) {
        BS_LOG(ERROR, "Make directory : [%s] FAILED", indexPath.c_str());
        return false;
    }

    if (!LineDataBuilder::storeSchema(indexPath, schema, FenceContext::NoFence())) {
        return false;
    }

    string targetFilePath = FileSystem::joinFilePath(indexPath, fileName);
    ec = FileSystem::isExist(targetFilePath);
    if (EC_TRUE == ec) {
        BS_LOG(INFO, "File[%s] exist.", targetFilePath.c_str());
        return true;
    } else if (EC_FALSE != ec) {
        return false;
    }

    if (!fslib::util::FileUtil::atomicCopy(file, targetFilePath)) {
        BS_LOG(ERROR, "Copy from path : [%s] to path : [%s] FAILED", file.c_str(), targetFilePath.c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::task_base
