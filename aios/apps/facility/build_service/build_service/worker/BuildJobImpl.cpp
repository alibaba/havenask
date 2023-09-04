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
#include "build_service/worker/BuildJobImpl.h"

#include "autil/StringUtil.h"
#include "build_service/builder/BuilderMetrics.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/task_base/BuildTask.h"
#include "build_service/util/Monitor.h"
#include "build_service/worker/ServiceWorker.h"
#include "build_service/workflow/FlowFactory.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/table/BuiltinDefine.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::common;
using namespace build_service::workflow;
using namespace build_service::task_base;
using namespace indexlib;

using namespace indexlib::index_base;
using namespace indexlib::config;

namespace build_service { namespace worker {
BS_LOG_SETUP(worker, BuildJobImpl);

BuildJobImpl::BuildJobImpl(const PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                           const LongAddress& address, const string& appZkRoot, const string& adminServiceName)
    : WorkerStateHandler(pid, metricProvider, appZkRoot, adminServiceName, "")
    , _buildFlow(NULL)
{
    _brokerFactory.reset(new workflow::FlowFactory({}, nullptr));
    *_current.mutable_longaddress() = address;
}

BuildJobImpl::~BuildJobImpl() { DELETE_AND_SET_NULL(_buildFlow); }

void BuildJobImpl::doHandleTargetState(const string& state, bool hasResourceUpdated)
{
    JobTarget target;
    if (!target.ParseFromString(state)) {
        BS_LOG(ERROR, "invalid job target string: %s", state.c_str());
        return;
    }

    // check target
    if (!target.has_configpath() || !target.has_datasourceid() || !target.has_datadescription()) {
        BS_LOG(ERROR, "job target (%s) invalid", target.ShortDebugString().c_str());
        return;
    }

    {
        ScopedLock lock(_lock);
        if (target.has_configpath()) {
            _configPath = target.configpath();
        }
    }

    BS_INTERVAL_LOG(300, INFO, "target status: %s", target.ShortDebugString().c_str());
    if (_current.targetstatus() == target) {
        return;
    }
    if (build(target)) {
        ScopedLock lock(_lock);
        *_current.mutable_targetstatus() = target;
    } else {
        FILL_ERRORINFO(BUILDER_ERROR_UNKNOWN, "build for target [" + target.ShortDebugString() + "]failed", BS_RETRY);
        setFatalError();
    }
}

void BuildJobImpl::getCurrentState(string& state)
{
    ScopedLock lock(_lock);
    if (isSuspended()) {
        _current.set_issuspended(true);
    }
    fillCurrentState(_buildFlow, _current);
    fillLocator(_buildFlow, _current);
    fillProtocolVersion(_current);
    saveCurrent(_current, state);
}

bool BuildJobImpl::hasFatalError()
{
    ScopedLock lock(_lock);
    return _hasFatalError || (_buildFlow && _buildFlow->hasFatalError());
}

bool BuildJobImpl::build(const JobTarget& target)
{
    string localConfigPath = downloadConfig(target.configpath());
    if (localConfigPath.empty()) {
        return false;
    }

    string msg = "start build job, target[" + target.ShortDebugString() + "]";
    BEEPER_INTERVAL_REPORT(60, WORKER_STATUS_COLLECTOR_NAME, msg);
    ResourceReaderPtr resourceReader = ResourceReaderManager::getResourceReader(localConfigPath);
    KeyValueMap kvMap;
    kvMap[CONFIG_PATH] = localConfigPath;

    // for job mode, we don't support branch && fence, keep index as before
    kvMap[BUILDER_BRANCH_NAME_LEGACY] = "true";

    if (!fillKeyValueMap(resourceReader, target, kvMap)) {
        FILL_ERRORINFO(BUILDER_ERROR_UNKNOWN, "fill kv map failed", BS_STOP);
        return false;
    }
    std::string tableType;
    if (!resourceReader->getTableType(_pid.clusternames(0), tableType)) {
        FILL_ERRORINFO(BUILDER_ERROR_UNKNOWN, "get table type failed", BS_STOP);
        return false;
    }
    if (indexlib::table::TABLE_TYPE_RAWFILE == tableType) {
        if (!BuildTask::buildRawFileIndex(resourceReader, kvMap, _pid)) {
            FILL_ERRORINFO(BUILDER_ERROR_UNKNOWN, "build rawfile failed", BS_RETRY);
            return false;
        }
        ScopedLock lock(_lock);
        _current.set_status(WS_STOPPED);
        return true;
    } else {
        ScopedLock lock(_lock);
        return startBuildFlow(resourceReader, kvMap);
    }
}

bool BuildJobImpl::fillKeyValueMap(const ResourceReaderPtr& resourceReader, const JobTarget& target, KeyValueMap& kvMap)
{
    BuildServiceConfig bsConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", bsConfig)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "failed to parse build_app.json", BS_STOP);
        return false;
    }

    CounterConfig& counterConfig = bsConfig.counterConfig;
    if (!overWriteCounterConfig(_pid, counterConfig)) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "rewrite CounterConfig failed", BS_STOP);
        return false;
    }
    kvMap[COUNTER_CONFIG_JSON_STR] = ToJsonString(counterConfig);

    // init generation meta
    if (!resourceReader->initGenerationMeta(_pid.buildid().generationid(), _pid.buildid().datatable(),
                                            bsConfig.getIndexRoot())) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "init generation meta failed", BS_STOP);
        return false;
    }

    // processor
    kvMap[SRC_SIGNATURE] = StringUtil::toString(target.datasourceid());
    string dataDescriptionStr = target.datadescription();
    DataDescription dataDescription;
    try {
        autil::legacy::FromJsonString(dataDescription, dataDescriptionStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        FILL_ERRORINFO(SERVICE_ERROR_CONFIG, "invalid datadescription string: " + target.datadescription(), BS_STOP);
        return false;
    }
    for (KeyValueMap::const_iterator it = dataDescription.begin(); it != dataDescription.end(); it++) {
        kvMap[it->first] = it->second;
    }

    kvMap[DATA_DESCRIPTION_KEY] = dataDescriptionStr;

    // builder
    // todo: in job mode support use different index root
    kvMap[INDEX_ROOT_PATH] = bsConfig.getIndexRoot();
    return true;
}

bool BuildJobImpl::startBuildFlow(const ResourceReaderPtr& resourceReader, KeyValueMap& kvMap)
{
    if (_buildFlow != NULL) {
        BS_LOG(WARN, "start twice");
        return false;
    }

    auto it = kvMap.find(COUNTER_CONFIG_JSON_STR);
    if (it == kvMap.end()) {
        BS_LOG(ERROR, "[%s] missing in kvMap", COUNTER_CONFIG_JSON_STR.c_str());
        return false;
    }

    CounterConfigPtr counterConfig(new CounterConfig());
    try {
        FromJsonString(*counterConfig.get(), it->second);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "jsonize [%s] failed, original str : [%s]", COUNTER_CONFIG_JSON_STR.c_str(), it->second.c_str());
        return false;
    }

    _buildFlow = createBuildFlow();
    BuildFlowMode mode = BuildFlow::getBuildFlowMode(_pid.role());
    if (!_buildFlow->startBuildFlow(resourceReader, _pid, kvMap, _brokerFactory.get(), mode, SERVICE,
                                    _metricProvider)) {
        fillErrorInfo(_buildFlow);
        AUTIL_LOG(ERROR, "start build failed, job exit");
        setFatalError();
        return false;
    }

    const auto& counterMap = _buildFlow->getCounterMap();
    if (counterMap) {
        _counterSynchronizer.reset(CounterSynchronizerCreator::create(counterMap, counterConfig));
        if (!_counterSynchronizer) {
            BS_LOG(ERROR, "fail to init counterSynchronizer");
            return false;
        } else if (!_counterSynchronizer->beginSync()) {
            BS_LOG(ERROR, "fail to start sync counter thread");
            _counterSynchronizer.reset();
            return false;
        }
    }
    return true;
}

workflow::BuildFlow* BuildJobImpl::createBuildFlow() { return new BuildFlow(); }

bool BuildJobImpl::needSuspendTask(const std::string& state)
{
    JobTarget target;
    if (!target.ParseFromString(state)) {
        string errorMsg = "failed to deserialize builder target state[" + state + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (target.has_suspendtask() && target.suspendtask() == true) {
        BS_LOG(WARN, "suspend task cmd received!");
        return true;
    }
    return false;
}

}} // namespace build_service::worker
