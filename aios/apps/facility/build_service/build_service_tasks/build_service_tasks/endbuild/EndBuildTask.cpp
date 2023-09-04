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
#include "build_service_tasks/endbuild/EndBuildTask.h"

#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::proto;
using namespace autil::legacy;
using namespace autil;

using namespace indexlib::index_base;
using namespace indexlib::merger;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, EndBuildTask);

const string EndBuildTask::TASK_NAME = BS_TASK_END_BUILD;
const string EndBuildTask::CHECKPOINT_NAME = "__ENDBUILD_CKP__";

EndBuildTask::EndBuildTask() : _finished(false) {}

EndBuildTask::~EndBuildTask() {}

bool EndBuildTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    if (initParam.clusterName.empty()) {
        string errorMsg = "lack of cluster name";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    string generationId = StringUtil::toString(_taskInitParam.pid.buildid().generationid());
    if (!generationId.empty()) {
        _options.AddVersionDesc(GENERATION_ID, generationId);
    }
    return true;
}

indexlib::util::CounterMapPtr EndBuildTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }

bool EndBuildTask::prepareParallelBuildInfo(const config::TaskTarget& target, ParallelBuildInfo& incBuildInfo) const
{
    string workerPathVersionStr;
    if (!target.getTargetDescription(BS_ENDBUILD_WORKER_PATHVERSION, workerPathVersionStr)) {
        BS_LOG(ERROR, "lack of worker path version");
        return false;
    }
    if (!StringUtil::fromString(workerPathVersionStr, incBuildInfo.batchId)) {
        BS_LOG(ERROR, "invalid worker path version[%s]", workerPathVersionStr.c_str());
        return false;
    }

    string buildParallelNum;
    if (!target.getTargetDescription(BUILD_PARALLEL_NUM, buildParallelNum)) {
        BS_LOG(ERROR, "lack of build parallel num");
        return false;
    }
    if (!StringUtil::fromString(buildParallelNum, incBuildInfo.parallelNum)) {
        BS_LOG(ERROR, "invalid build parallel num[%s]", buildParallelNum.c_str());
        return false;
    }

    incBuildInfo.instanceId = 0;
    return true;
}

void EndBuildTask::prepareIndexOptions(const config::TaskTarget& target)
{
    string opIdsStr;
    if (target.getTargetDescription(OPERATION_IDS, opIdsStr) && !opIdsStr.empty()) {
        vector<schema_opid_t> opIds;
        StringUtil::fromString(opIdsStr, opIds, ",");
        _options.SetOngoingModifyOperationIds(opIds);
    }

    string schemaPatch;
    if (target.getTargetDescription(SCHEMA_PATCH, schemaPatch) && !schemaPatch.empty()) {
        UpdateableSchemaStandards standards;
        try {
            FromJsonString(standards, schemaPatch);
            _options.SetUpdateableSchemaStandards(standards);
        } catch (...) {
            IE_LOG(ERROR, "invalid schema patch [%s]", schemaPatch.c_str());
        }
    }

    string batchMaskStr;
    if (target.getTargetDescription(BATCH_MASK, batchMaskStr) && !batchMaskStr.empty()) {
        int32_t batchMask;
        if (!StringUtil::fromString(batchMaskStr, batchMask) || batchMask < 0) {
            BS_LOG(ERROR, "invalid batch mask [%s]", batchMaskStr.c_str());
            return;
        }
        _options.AddVersionDesc(BATCH_MASK, batchMaskStr);
    }
}

bool EndBuildTask::handleTarget(const config::TaskTarget& target)
{
    // when current target is finish, but receive a new target, worker should restart to do new target
    {
        ScopedLock lock(_lock);
        if (_finished && target != _currentFinishTarget) {
            return false;
        }
        if (_finished) {
            return true;
        }
    }
    ParallelBuildInfo incBuildInfo;
    if (!prepareParallelBuildInfo(target, incBuildInfo)) {
        return false;
    }

    prepareIndexOptions(target);

    // get index root
    IndexPartitionSchemaPtr schema = _taskInitParam.resourceReader->getSchema(_taskInitParam.clusterName);
    BuildServiceConfig serviceConfig;
    if (!_taskInitParam.resourceReader->getConfigWithJsonPath(BUILD_APP_FILE_NAME, "", serviceConfig)) {
        string errorMsg =
            "failed to parse build_app.json, configPath is [" + _taskInitParam.resourceReader->getConfigPath() + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    string targetIndexPath = IndexPathConstructor::constructIndexPath(serviceConfig.getIndexRoot(), _taskInitParam.pid);
    try {
        vector<string> mergeSrc = ParallelPartitionDataMerger::GetParallelInstancePaths(targetIndexPath, incBuildInfo);
        indexlib::merger::ParallelPartitionDataMerger dataMerger(targetIndexPath, _taskInitParam.epochId, schema,
                                                                 _options);
        // always try to MergeSegmentData, is dir is empty or not exist, will tell admin task is finished
        Version version = dataMerger.MergeSegmentData(mergeSrc);
        // TODO: admin should send target version timestamp to endBuild worker
        //       check final version ts is the equal to target version timestamp
        if (version.GetVersionId() == INVALID_VERSION) {
            BS_LOG(ERROR, "merge parallel data failed.");
            return false;
        }
        {
            BS_LOG(INFO, "prepare remove parallel dir...");
            ScopedLock lock(_lock);
            // should remove directory first before telling admin task is finished.
            dataMerger.RemoveParallBuildDirectory();
            _kvMap[BS_ENDBUILD_VERSION] = StringUtil::toString(version.GetVersionId());
            _finished = true;
            _currentFinishTarget = target;
            return true;
        }
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "end build failed, exception[" << string(e.what()) << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return false;
}

bool EndBuildTask::isDone(const config::TaskTarget& target)
{
    ScopedLock lock(_lock);
    return _finished && _currentFinishTarget == target;
}

string EndBuildTask::getTaskStatus()
{
    ScopedLock lock(_lock);
    return ToJsonString(_kvMap);
}

}} // namespace build_service::task_base
