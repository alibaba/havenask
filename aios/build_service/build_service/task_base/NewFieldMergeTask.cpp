#include "build_service/task_base/NewFieldMergeTask.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/custom_merger/TaskItemDispatcher.h"
#include "build_service/custom_merger/MergeResourceProvider.h"
#include "build_service/util/FileUtil.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include <indexlib/merger/parallel_partition_data_merger.h>
#include <indexlib/index_base/schema_adapter.h>
#include <beeper/beeper.h>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::custom_merger;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(merger);

namespace build_service {
namespace task_base {
BS_LOG_SETUP(task_base, NewFieldMergeTask);

NewFieldMergeTask::NewFieldMergeTask(
    IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
    : TaskBase(metricProvider)
    , _targetVersionId(-1)
    , _alterFieldTargetVersionId(-1)
    , _checkpointVersionId(-1)
    , _opsId(0)
{
}

NewFieldMergeTask::~NewFieldMergeTask() {
}

bool NewFieldMergeTask::init(const std::string &jobParam)  {
    if (!TaskBase::init(jobParam)) {
        return false;
    }
    string clusterName = _jobConfig.getClusterName();
    string clusterConfig = _resourceReader->getClusterConfRelativePath(clusterName);
    
    if (!_resourceReader->getConfigWithJsonPath(clusterConfig,
                    "offline_index_config.customized_merge_config.alter_field", _offlineMergeConfig))
    {
        BS_LOG(ERROR, "alter_field config not exist!");
        return false;
    }
    _customMergerCreator.reset(new CustomMergerCreator);
    if (!parseTargetDescription()) {
        BS_LOG(ERROR, "get target_description info failed!");
        return false;
    }

    auto schema = _resourceReader->getSchema(clusterName);
    if (_opsId != 0) {
        _newSchema.reset(schema->CreateSchemaForTargetModifyOperation(_opsId));
    } else {
        _newSchema = schema;
    }
    return _customMergerCreator->init(_resourceReader, _metricProvider);
}

bool NewFieldMergeTask::parseTargetDescription() {
    string opsId = getValueFromKeyValueMap(_kvMap, "operationId");
    if (opsId.empty()) {
        return true;
    }
    string targetVersion = getValueFromKeyValueMap(_kvMap, "targetVersion");
    string checkpointVersion = getValueFromKeyValueMap(_kvMap, "checkpointVersion");

    if (!StringUtil::fromString(targetVersion, _alterFieldTargetVersionId) ||
        !StringUtil::fromString(checkpointVersion, _checkpointVersionId)||
        !StringUtil::fromString(opsId, _opsId)) {
        return false;
    }
    if (_opsId <= 0) {
        return false;
    }
    if (_alterFieldTargetVersionId < 0) {
        return false;
    }
    if (_alterFieldTargetVersionId <= _checkpointVersionId) {
        return false;
    }
    return true;
}

MergeResourceProviderPtr NewFieldMergeTask::prepareResourceProvider() {
    MergeResourceProviderPtr provider(new MergeResourceProvider());
    if (provider->init(_indexRoot, _mergeIndexPartitionOptions,
                       _newSchema, _resourceReader->getPluginPath(),
                       _alterFieldTargetVersionId, _checkpointVersionId)) {
        return provider;
    }
    return MergeResourceProviderPtr();
}

bool NewFieldMergeTask::run(uint32_t instanceId, Mode mode) {
    PartitionId partitionId = createPartitionId(instanceId, mode, "merger");
    BS_LOG(INFO, "instanceId[%u] %s", instanceId, partitionId.ShortDebugString().c_str());
    string indexRoot = getValueFromKeyValueMap(_kvMap, INDEX_ROOT_PATH);
    _indexRoot = IndexPathConstructor::constructIndexPath(indexRoot, partitionId);
    
    try {
        if (mode == MERGE_MAP) {
            return beginMerge();
        } else if (mode == MERGE_REDUCE) {
            const BuildRuleConfig &buildRuleConf =
                _jobConfig.getBuildRuleConf();
            return doMerge(instanceId % buildRuleConf.mergeParallelNum);
        } else if (mode == END_MERGE_MAP) {
            return endMerge();
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        string errorMsg = "Merge fail, exception " + string(e.what());
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        return false;
    } catch (...) {
        string errorMsg = "Merge fail, unknown exception";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        BEEPER_REPORT(WORKER_ERROR_COLLECTOR_NAME, errorMsg);
        return false;
    }
    return true;
}

bool NewFieldMergeTask::beginMerge() {
    BuildStep buildStep = BUILD_STEP_FULL;
    if (!getBuildStep(buildStep) || BUILD_STEP_FULL == buildStep) {
        BS_LOG(ERROR, "unknown build step or build step is full!");
        return false;
    }
    int32_t workerPathVersion = -1;
    if (!getWorkerPathVersion(workerPathVersion)) {
        return false;
    }
    
    if (workerPathVersion < 0 || _opsId != 0) {
        // legacy admin heartbeat
        // do nothing
        //parallel alter field not mv parallel build data
    }
    else {
        uint32_t parallelNum = 0;
        if (!getIncBuildParallelNum(parallelNum))
        {
            return false;
        }
        ParallelBuildInfo incBuildInfo;
        incBuildInfo.parallelNum = parallelNum;
        incBuildInfo.batchId = workerPathVersion;
        incBuildInfo.instanceId = 0; // not care instanceId 
        mergeIncParallelBuildData(incBuildInfo);
    }

    _newFieldMerger = createCustomMerger();
    if (!_newFieldMerger) {
        return false;
    }
    MergeResourceProviderPtr resourceProvider = _newFieldMerger->getResourceProvider();
    assert(resourceProvider);
    if (_opsId == 0) { 
        _targetVersionId =
            resourceProvider->getPartitionResourceProvider()->GetVersion().GetVersionId() + 1;
    } else {
        _targetVersionId = _alterFieldTargetVersionId;
    }
    return prepareNewFieldMergeMeta(resourceProvider);
}

bool NewFieldMergeTask::doMerge(uint32_t instanceId) {
    _newFieldMerger = createCustomMerger();
    if (!_newFieldMerger) {
        return false;
    }
    CustomMergeMeta mergeMeta;
    mergeMeta.load(getMergeMetaPath());
    return _newFieldMerger->merge(mergeMeta, instanceId, _indexRoot);
}

bool NewFieldMergeTask::prepareNewFieldMergeMeta(const MergeResourceProviderPtr& resourceProvider) {
    vector<CustomMergerTaskItem> items;
    if (!_newFieldMerger->estimateCost(items)) {
        return false;
    }
    vector<TaskItemDispatcher::SegmentInfo> segmentInfos;
    resourceProvider->getIndexSegmentInfos(segmentInfos);
    const BuildRuleConfig &buildRuleConf =
        _jobConfig.getBuildRuleConf();
    uint32_t totalInstanceCount = buildRuleConf.mergeParallelNum;

    CustomMergeMeta mergeMeta;
    TaskItemDispatcher::DispatchTasks(segmentInfos, items, totalInstanceCount, mergeMeta);
    mergeMeta.store(getMergeMetaPath());
    return true;
}

std::string NewFieldMergeTask::getMergeMetaPath() {
    string mergeMetaRoot =
        util::FileUtil::joinFilePath(_indexRoot, "alter_new_field");
    string metaDirName = _kvMap[MERGE_TIMESTAMP];
    if (_opsId != 0) {
        metaDirName = metaDirName + "_" + StringUtil::toString(_opsId);
    }
    string mergeMetaVersionDir = 
        util::FileUtil::joinFilePath(mergeMetaRoot, metaDirName);
    
    return mergeMetaVersionDir;
}

bool NewFieldMergeTask::endMerge() {
    _newFieldMerger = createCustomMerger();
    if (!_newFieldMerger) {
        return false;
    }

    MergeResourceProviderPtr resourceProvider = _newFieldMerger->getResourceProvider();
    assert(resourceProvider);
    if (_opsId ==0) {
        ParallelPartitionDataMerger dataMerger(_indexRoot, resourceProvider->getOldSchema());
        dataMerger.RemoveParallBuildDirectory();
    }

    CustomMergeMeta mergeMeta;
    mergeMeta.load(getMergeMetaPath());
    if (_opsId != 0) {
        return _newFieldMerger->endMerge(mergeMeta, _indexRoot, -1);
    }
    string alignedVersionStr = getValueFromKeyValueMap(_kvMap, ALIGNED_VERSION_ID, "-1");
    versionid_t alignedVersionId;
    if (!StringUtil::numberFromString(alignedVersionStr, alignedVersionId)) {
        BS_LOG(ERROR, "invalid aligned version id: %s", alignedVersionStr.c_str());
        return false;
    }
    if (resourceProvider->getVersion().GetVersionId() == alignedVersionId) {
        BS_LOG(INFO, "endmerge task has done."
               " Do nothing and return directly.version id: %d", alignedVersionId);
        return true;
    }

    return _newFieldMerger->endMerge(mergeMeta, _indexRoot, alignedVersionId);
}

void NewFieldMergeTask::mergeIncParallelBuildData(const ParallelBuildInfo& incBuildInfo)
{
    ParallelPartitionDataMerger dataMerger(_indexRoot, SchemaAdapter::LoadSchema(_indexRoot));
    vector<string> mergeSrc = ParallelPartitionDataMerger::GetParallelInstancePaths(
            _indexRoot, incBuildInfo);
    dataMerger.MergeSegmentData(mergeSrc);
}

CustomMergerPtr NewFieldMergeTask::createCustomMerger() {
    MergeResourceProviderPtr resourceProvider = prepareResourceProvider();
    if (!resourceProvider) {
        BS_LOG(ERROR, "create merge resource provider failed");
        return CustomMergerPtr();
    }

    CustomMergerPtr newFieldMerger = 
        _customMergerCreator->create(_offlineMergeConfig.mergePluginConfig, resourceProvider);
    if (!newFieldMerger) {
        BS_LOG(ERROR, "create custom merger failed, %s",
               ToJsonString(_offlineMergeConfig.mergePluginConfig).c_str());
    }
    return newFieldMerger;
}

}
}
