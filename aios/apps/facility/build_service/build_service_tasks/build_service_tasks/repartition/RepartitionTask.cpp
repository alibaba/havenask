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
#include "build_service_tasks/repartition/RepartitionTask.h"

#include <assert.h>
#include <iosfwd>
#include <memory>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/HashModeConfig.h"
#include "build_service/config/IndexPartitionOptionsWrapper.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "build_service_tasks/repartition/RepartitionDocFilterCreator.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/module_info.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/merger/doc_filter.h"
#include "indexlib/merger/doc_filter_creator.h"
#include "indexlib/merger/merge_meta.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::proto;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::merger;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, RepartitionTask);

const string RepartitionTask::TASK_NAME = BS_REPARTITION;
RepartitionTask::RepartitionTask() {}

RepartitionTask::~RepartitionTask() {}

bool RepartitionTask::init(TaskInitParam& initParam)
{
    _initParam = initParam;
    auto ranges = RangeUtil::splitRange(0, 65535, _initParam.instanceInfo.partitionCount);
    if (_initParam.instanceInfo.partitionId >= ranges.size()) {
        BS_LOG(ERROR, "out of range, invalid partition id [%u]", _initParam.instanceInfo.partitionId);
        return false;
    }
    _partitionRange = ranges[_initParam.instanceInfo.partitionId];
    BuildServiceConfig serviceConfig;
    if (!_initParam.resourceReader->getConfigWithJsonPath("build_app.json", "", serviceConfig)) {
        string errorMsg = "failed to get build_app.json in config[" + _initParam.resourceReader->getConfigPath() + "]";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    _srcIndexRoot = serviceConfig.getIndexRoot();
    return true;
}

bool RepartitionTask::isDone(const config::TaskTarget& targetDescription)
{
    ScopedLock lock(_lock);
    if (targetDescription == _currentFinishTarget) {
        return true;
    }
    return false;
}

bool RepartitionTask::handleTarget(const config::TaskTarget& target)
{
    if (isDone(target)) {
        return true;
    }

    try {
        string targetIndexPath;
        if (!target.getTargetDescription("indexPath", targetIndexPath)) {
            BS_LOG(ERROR, "no index path from target");
            return false;
        }
        uint32_t generationId;
        if (!target.getTargetDescription("generationId", generationId)) {
            BS_LOG(ERROR, "no generation id from target");
            return false;
        }
        uint32_t parallelNum;
        if (!target.getTargetDescription("parallelNum", parallelNum)) {
            BS_LOG(ERROR, "no parallelNum from target");
            return false;
        }

        string targetGenerationPath =
            IndexPathConstructor::getGenerationIndexPath(targetIndexPath, _initParam.clusterName, generationId);
        bool isExist = false;
        if (!fslib::util::FileUtil::isExist(targetGenerationPath, isExist)) {
            BS_LOG(ERROR, "visit target generation path [%s] failed", targetGenerationPath.c_str());
            return false;
        }
        if (!isExist) {
            if (!fslib::util::FileUtil::mkDir(targetGenerationPath, true)) {
                BS_LOG(ERROR, "prepare target generation path [%s] failed", targetGenerationPath.c_str());
                return false;
            }
        }

        config::TaskTarget checkPoint;
        if (!readCheckpoint(targetGenerationPath, checkPoint)) {
            BS_LOG(ERROR, "read checkpoint failed");
            return false;
        }

        if (checkPoint == target) {
            BS_INTERVAL_LOG(300, INFO, "repartition task has been done");
            ScopedLock lock(_lock);
            _currentFinishTarget = target;
            return true;
        }

        versionid_t version;
        if (!target.getTargetDescription(BS_SNAPSHOT_VERSION, version)) {
            BS_LOG(ERROR, "target not has snapshot version: original str : [%s]", ToJsonString(target).c_str());
            return false;
        }
        string targetName = target.getTargetName();
        if (targetName != "begin_task" && targetName != "do_task" && targetName != "end_task") {
            BS_LOG(ERROR, "target name not support: [%s]", targetName.c_str());
            return false;
        }

        FilteredMultiPartitionMergerPtr merger =
            prepareFilteredMultiPartitionMerger(version, targetIndexPath, generationId);
        if (!merger) {
            return false;
        }
        string mergeMetaRoot = merger->GetMergeMetaDir();
        string timestamp;
        target.getTargetDescription("timestamp", timestamp);
        string mergeMetaVersionDir = fslib::util::FileUtil::joinFilePath(mergeMetaRoot, timestamp);

        if (targetName == "begin_task") {
            merger->PrepareMergeMeta(parallelNum, mergeMetaVersionDir);
        } else if (targetName == "do_task") {
            if (!doMerge(merger, mergeMetaVersionDir)) {
                return false;
            }
        } else if (targetName == "end_task") {
            if (!endMerge(merger, mergeMetaVersionDir)) {
                return false;
            }
        } else {
            assert(false);
            return false;
        }
        if (!checkInCheckpoint(targetGenerationPath, target)) {
            return false;
        }
        {
            ScopedLock lock(_lock);
            _currentFinishTarget = target;
            return true;
        }
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "%s", e.what());
        return false;
    }
}

bool RepartitionTask::endMerge(const FilteredMultiPartitionMergerPtr& merger, const std::string& mergeMetaVersionDir)
{
    MergeMetaPtr mergeMeta = merger->LoadMergeMeta(mergeMetaVersionDir, true);
    if (!mergeMeta) {
        string errorMsg = "Load merge meta file: [" + mergeMetaVersionDir + "] FAILED.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    merger->EndMerge(mergeMeta, indexlib::INVALID_VERSIONID);
    return true;
}

bool RepartitionTask::doMerge(const FilteredMultiPartitionMergerPtr& merger, const std::string& mergeMetaVersionDir)
{
    MergeMetaPtr mergeMeta = merger->LoadMergeMeta(mergeMetaVersionDir, false);
    if (!mergeMeta) {
        string errorMsg = "Load merge meta file: [" + mergeMetaVersionDir + "] FAILED.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    merger->DoMerge(mergeMeta, _initParam.instanceInfo.instanceId);
    return true;
}

bool RepartitionTask::checkInCheckpoint(const std::string& targetGenerationPath, const config::TaskTarget& target)
{
    string checkpointFile = fslib::util::FileUtil::joinFilePath(targetGenerationPath, getCheckpointFileName());
    return fslib::util::FileUtil::writeWithBak(checkpointFile, ToJsonString(target));
}

FilteredMultiPartitionMergerPtr RepartitionTask::prepareFilteredMultiPartitionMerger(versionid_t version,
                                                                                     const std::string& targetIndexPath,
                                                                                     uint32_t targetGenerationId)
{
    HashModeConfigPtr hashModeConfig(new HashModeConfig);
    IndexPartitionSchemaPtr schema = _initParam.resourceReader->getSchema(_initParam.clusterName);
    if (!hashModeConfig->init(_initParam.clusterName, _initParam.resourceReader.get(),
                              std::make_shared<indexlib::config::LegacySchemaAdapter>(schema))) {
        BS_LOG(ERROR, "create hashModeConfig failed");
        return FilteredMultiPartitionMergerPtr();
    }
    RepartitionDocFilterCreatorPtr docFilterCreator(new RepartitionDocFilterCreator(_partitionRange, hashModeConfig));

    IndexPartitionOptions fullMergeOptions;
    if (!getFullMergeOptions(fullMergeOptions)) {
        BS_LOG(ERROR, "get full merge config fail");
        return FilteredMultiPartitionMergerPtr();
    }
    FilteredMultiPartitionMergerPtr merger(new FilteredMultiPartitionMerger(fullMergeOptions, _initParam.metricProvider,
                                                                            _initParam.resourceReader->getPluginPath(),
                                                                            docFilterCreator, _initParam.epochId));
    string rangeFrom = StringUtil::toString(_partitionRange.from());
    string rangeTo = StringUtil::toString(_partitionRange.to());
    string generationId = StringUtil::toString(targetGenerationId);
    string mergeDest = IndexPathConstructor::constructIndexPath(targetIndexPath, _initParam.clusterName, generationId,
                                                                rangeFrom, rangeTo);
    vector<string> mergePartPaths;
    collectMergePartPaths(mergePartPaths);
    merger->Init(mergePartPaths, version, mergeDest);
    return merger;
}

bool RepartitionTask::getFullMergeOptions(IndexPartitionOptions& options)
{
    string fullMergeConfig;
    if (!IndexPartitionOptionsWrapper::getFullMergeConfigName(
            _initParam.resourceReader.get(), _initParam.buildId.dataTable, _initParam.clusterName, fullMergeConfig)) {
        return false;
    }
    IndexPartitionOptionsWrapper wrapper;
    if (!wrapper.load(*(_initParam.resourceReader), _initParam.clusterName)) {
        return false;
    }
    if (!wrapper.hasIndexPartitionOptions(fullMergeConfig)) {
        return false;
    }
    if (!wrapper.getIndexPartitionOptions(fullMergeConfig, options)) {
        return false;
    }
    options.SetIsOnline(false);
    return true;
}

bool RepartitionTask::collectMergePartPaths(vector<string>& paths)
{
    BuildRuleConfig buildRuleConfig;
    if (!_initParam.resourceReader->getClusterConfigWithJsonPath(
            _initParam.clusterName, "cluster_config.builder_rule_config", buildRuleConfig)) {
        string errorMsg = "parse cluster_config.builder_rule_config for [" + _initParam.clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    int32_t partitionCount = buildRuleConfig.partitionCount;
    auto ranges = RangeUtil::splitRange(0, 65535, partitionCount);

    proto::BuildId buildId;
    buildId.set_datatable(_initParam.buildId.dataTable);
    buildId.set_generationid(_initParam.buildId.generationId);
    buildId.set_appname(_initParam.buildId.appName);

    vector<proto::Range> targetRanges;
    for (auto range : ranges) {
        if (range.from() > _partitionRange.to() || range.to() < _partitionRange.from()) {
            continue;
        }
        PartitionId pid;
        *pid.mutable_range() = range;
        *pid.mutable_buildid() = buildId;
        *pid.add_clusternames() = _initParam.clusterName;
        string partitionPath = IndexPathConstructor::constructIndexPath(_srcIndexRoot, pid);
        paths.push_back(partitionPath);
    }
    return true;
}

string RepartitionTask::getCheckpointFileName()
{
    return string("task.repartition.partitionId_") + StringUtil::toString(_initParam.instanceInfo.partitionId) +
           ".instanceId_" + StringUtil::toString(_initParam.instanceInfo.instanceId) + ".checkpoint";
}

bool RepartitionTask::readCheckpoint(const std::string& targetGenerationPath, config::TaskTarget& checkpoint)
{
    string checkpointFile = fslib::util::FileUtil::joinFilePath(targetGenerationPath, getCheckpointFileName());
    string content;
    if (!fslib::util::FileUtil::readWithBak(checkpointFile, content)) {
        BS_LOG(ERROR, "read checkpoint file[%s] failed", checkpointFile.c_str());
        return false;
    }

    if (content.empty()) {
        BS_LOG(INFO, "checkpoint file[%s] does not exist", checkpointFile.c_str());
        return true;
    } else {
        if (!parseTaskTarget(content, checkpoint)) {
            string errorMsg = "translate json string[" + content + "]to taskTarget failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
    }
    return true;
}

bool RepartitionTask::parseTaskTarget(const std::string& targetStr, config::TaskTarget& target)
{
    try {
        FromJsonString(target, targetStr);
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "jsonize targetDescription failed, original str : [%s]", targetStr.c_str());
        return false;
    }
    return true;
}

indexlib::util::CounterMapPtr RepartitionTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }

std::string RepartitionTask::getTaskStatus() { return ""; }
}} // namespace build_service::task_base
