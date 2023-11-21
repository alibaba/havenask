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
#include "build_service/local_job/LocalJobWorker.h"

#include <iostream>
#include <memory>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Span.h"
#include "autil/ThreadPool.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/ConfigDownloader.h"
#include "build_service/common_define.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/local_job/BuildInstanceWorkItem.h"
#include "build_service/local_job/MergeInstanceWorkItem.h"
#include "build_service/local_job/MergeInstanceWorkItemV2.h"
#include "build_service/local_job/ReduceDocumentQueue.h"
#include "build_service/task_base/JobConfig.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/common_branch_hinter.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::task_base;
using namespace build_service::config;
using namespace build_service::common;

namespace build_service { namespace local_job {

BS_LOG_SETUP(local_job, LocalJobWorker);

static const string BRANCH_POLICY_ENV = "BRANCH_NAME_POLICY";
LocalJobWorker::LocalJobWorker(bool useV2Build) : _useV2Build(useV2Build)
{
    // this mode not use branch fs to build
    if (!autil::EnvUtil::hasEnv(BRANCH_POLICY_ENV)) {
        if (autil::EnvUtil::setEnv(BRANCH_POLICY_ENV, indexlib::index_base::CommonBranchHinter::Option::BNP_LEGACY,
                                   true)) {
            _resetEnv = true;
        }
    }
}

LocalJobWorker::~LocalJobWorker()
{
    if (_resetEnv) {
        autil::EnvUtil::unsetEnv("BRANCH_NAME_POLICY");
    }
}
bool LocalJobWorker::run(const string& step, uint16_t mapCount, uint16_t reduceCount, const string& jobParams)
{
    BS_LOG(INFO, "mapCount[%u] reduceCount[%u]", mapCount, reduceCount);

    KeyValueMap kvMap;
    try {
        FromJsonString(kvMap, jobParams);
    } catch (const ExceptionBase& e) {
        string errorMsg = "Invalid job param[" + jobParams + "], from json failed[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    std::string configPath = getValueFromKeyValueMap(kvMap, CONFIG_PATH);
    std::string localConfigPath;
    if (!downloadConfig(configPath, localConfigPath)) {
        BS_LOG(ERROR, "download config from %s failed", configPath.c_str());
        return false;
    }
    string clusterName = getValueFromKeyValueMap(kvMap, CLUSTER_NAMES);
    config::ResourceReader resourceReader(localConfigPath);
    auto schema = resourceReader.getTabletSchema(clusterName);
    if (!schema) {
        BS_LOG(ERROR, "load schema for cluster %s failed", clusterName.c_str());
        return false;
    }
    if (!schema->GetLegacySchema() || schema->GetLegacySchema()->IsTablet()) {
        _useV2Build = true;
    }
    bool ret = false;
    if ("build" == step) {
        ret = buildJob(mapCount, reduceCount, jobParams, _useV2Build);
    } else if ("merge" == step) {
        ret = mergeJob(mapCount, reduceCount, jobParams, _useV2Build);
    } else if ("endmerge" == step) {
        if (_useV2Build) {
            BS_LOG(INFO, "end merge is not supported for tablet");
            ret = true;
        } else {
            ret = endMergeJob(mapCount, reduceCount, jobParams);
        }
    } else {
        string errorMsg = "unrecognized job step [" + step + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        ret = false;
    }
    BS_LOG(INFO, "run step [%s] %s", step.c_str(), ret ? "success" : "failed");
    return ret;
}

bool LocalJobWorker::buildJob(uint16_t mapCount, uint16_t reduceCount, const string& jobParams, bool isTablet)
{
    ReduceDocumentQueue queue(reduceCount);
    bool failflag = false;

    ThreadPool mapThreadPool(mapCount);
    mapThreadPool.start();
    for (uint16_t instanceId = 0; instanceId < mapCount; instanceId++) {
        BuildInstanceWorkItem* workitem =
            new BuildInstanceWorkItem(&failflag, TaskBase::BUILD_MAP, instanceId, jobParams, &queue, isTablet);
        mapThreadPool.pushWorkItem(workitem, true);
    }

    ThreadPool reduceThreadPool(reduceCount);
    reduceThreadPool.start();
    for (uint16_t instanceId = 0; instanceId < reduceCount; instanceId++) {
        BuildInstanceWorkItem* workitem =
            new BuildInstanceWorkItem(&failflag, TaskBase::BUILD_REDUCE, instanceId, jobParams, &queue, isTablet);
        reduceThreadPool.pushWorkItem(workitem, true);
    }

    mapThreadPool.stop();
    queue.setFinished();
    reduceThreadPool.stop();

    if (failflag) {
        return false;
    }

    return true;
}

bool LocalJobWorker::mergeJob(uint16_t mapCount, uint16_t reduceCount, const string& jobParams, bool isTablet)
{
    if (isTablet) {
        return parallelRunMergeTask(mapCount, jobParams, TaskBase::MERGE_MAP, isTablet);
    }
    return parallelRunMergeTask(mapCount, jobParams, TaskBase::MERGE_MAP, isTablet) &&
           parallelRunMergeTask(reduceCount, jobParams, TaskBase::MERGE_REDUCE, isTablet);
}

bool LocalJobWorker::endMergeJob(uint16_t mapCount, uint16_t reduceCount, const string& jobParams)
{
    return parallelRunMergeTask(mapCount, jobParams, TaskBase::END_MERGE_MAP) &&
           parallelRunMergeTask(reduceCount, jobParams, TaskBase::END_MERGE_REDUCE);
}

bool LocalJobWorker::parallelRunMergeTask(size_t instanceCount, const string& jobParams, TaskBase::Mode mode,
                                          bool isTablet)
{
    bool failflag = false;
    ThreadPool threadPool(1);
    threadPool.start();
    for (uint16_t instanceId = 0; instanceId < instanceCount; instanceId++) {
        if (isTablet) {
            MergeInstanceWorkItemV2* workitem = new MergeInstanceWorkItemV2(&failflag, mode, instanceId, jobParams);
            threadPool.pushWorkItem(workitem, true);
        } else {
            MergeInstanceWorkItem* workitem = new MergeInstanceWorkItem(&failflag, mode, instanceId, jobParams);
            threadPool.pushWorkItem(workitem, true);
        }
    }
    threadPool.stop();

    return !failflag;
}

bool LocalJobWorker::downloadConfig(const std::string& configPath, std::string& localConfigPath)
{
    ConfigDownloader::DownloadErrorCode errorCode = ConfigDownloader::downloadConfig(configPath, localConfigPath);
    if (errorCode != ConfigDownloader::DEC_NONE) {
        string errorMsg = "download config[" + configPath + "] to local failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::local_job
