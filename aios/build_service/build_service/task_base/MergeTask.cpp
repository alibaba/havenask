#include "build_service/task_base/MergeTask.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/common/CounterSynchronizerCreator.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/FileUtil.h"
#include "build_service/proto/ProtoUtil.h"
#include <indexlib/util/counter/counter_map.h>
#include <indexlib/index_base/version_loader.h>
#include <indexlib/index_base/version_committer.h>
#include <indexlib/index_base/schema_adapter.h>
#include <indexlib/index_base/index_meta/parallel_build_info.h>
#include <indexlib/merger/multi_partition_merger.h>
#include <indexlib/merger/sorted_index_partition_merger.h>
#include <indexlib/merger/partition_merger_creator.h>
#include <autil/TimeUtility.h>
#include <beeper/beeper.h>

using namespace std;
using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::config;
using namespace autil;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(misc);

namespace build_service {
namespace task_base {
BS_LOG_SETUP(task_base, MergeTask);

const uint32_t MergeTask::RESERVED_VERSION_NUM = 5;

MergeTask::MergeTask()
{
    assert(!_metricProvider);
}

MergeTask::MergeTask(IE_NAMESPACE(misc)::MetricProviderPtr metricProvider)
    : TaskBase(metricProvider)
{
    assert(_metricProvider);
}

MergeTask::~MergeTask() {
    cleanUselessResource();
}

bool MergeTask::run(const string &jobParam, uint32_t instanceId,
                    Mode mode, bool optimize)
{
    if (!TaskBase::init(jobParam)) {
        return false;
    }

    // COUNTER_CONFIG_JSON_STR will not be set in bs_local_job
    CounterConfigPtr counterConfig;
    auto it = _kvMap.find(COUNTER_CONFIG_JSON_STR);
    if (it == _kvMap.end()) {
        BS_LOG(ERROR, "[%s] missing in jobParam", COUNTER_CONFIG_JSON_STR.c_str());
    } else {
        counterConfig.reset(new CounterConfig());
        try {
            FromJsonString(*counterConfig.get(), it->second);
        } catch (const ExceptionBase &e) {
            BS_LOG(ERROR, "jsonize [%s] failed, original str : [%s]",
                   COUNTER_CONFIG_JSON_STR.c_str(), it->second.c_str());
            return false;
        }
    }
    
    pair<MergeStep, uint32_t> mergeStep = getMergeStep(mode, instanceId);
    if (mergeStep.first == MS_DO_NOTHING) {
        return true;
    }
    PartitionId partitionId = createPartitionId(instanceId, mode, "merger");
    BS_LOG(INFO, "instanceId[%u] %s", instanceId, partitionId.ShortDebugString().c_str());
    if (!_metricProvider && !startMonitor(partitionId.range(), mode)) {
        string errorMsg = "start monitor failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    string indexRoot = getValueFromKeyValueMap(_kvMap, INDEX_ROOT_PATH);
    string targetIndexPath = IndexPathConstructor::constructIndexPath(indexRoot, partitionId);
    bool needMergeMultiPart = isMultiPartMerge();
    try {
        if (needMergeMultiPart) {
            string builderIndexRoot = getValueFromKeyValueMap(_kvMap, BUILDER_INDEX_ROOT_PATH, indexRoot);
            return mergeMultiPart(partitionId, builderIndexRoot, targetIndexPath,
                    _mergeIndexPartitionOptions, mergeStep, optimize, counterConfig);
        } else {
            return mergeSinglePart(partitionId, targetIndexPath,
                    _mergeIndexPartitionOptions, mergeStep, optimize, counterConfig);
        }
    } catch (const ExceptionBase &e) {
        BS_LOG(ERROR, "%s", e.what());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "%s", "unknown exception");
        return false;
    }
}

bool MergeTask::mergeMultiPart(const PartitionId &partitionId,
                               const string &indexRoot,
                               const string &targetIndexPath,
                               const IndexPartitionOptions &options,
                               const pair<MergeStep, uint32_t> mergeStep,
                               bool optimize,
                               const CounterConfigPtr &counterConfig)
{
    vector<Range> mergePartRanges = _jobConfig.getAllNeedMergePart(partitionId.range());
    vector<string> mergePartPaths;
    for (size_t i = 0; i < mergePartRanges.size(); ++i) {
        PartitionId toMergePartId = partitionId;
        *toMergePartId.mutable_range() = mergePartRanges[i];
        mergePartPaths.push_back(IndexPathConstructor::constructIndexPath(
                        indexRoot, toMergePartId));
    }
    const string& clusterName = _jobConfig.getClusterName();
    IndexPartitionSchemaPtr schema = _resourceReader->getSchema(clusterName);
    if (!schema) {
        IE_LOG(ERROR, "get schema for cluster[%s] failed", clusterName.c_str());
        return false;
    }
    IndexPartitionMergerPtr merger;
    IE_NAMESPACE(PartitionRange) targetRange(partitionId.range().from(), partitionId.range().to());
    try
    {
        merger.reset(
            (IndexPartitionMerger*)PartitionMergerCreator::CreateFullParallelPartitionMerger(
                mergePartPaths, targetIndexPath, options,
                _metricProvider, _resourceReader->getPluginPath(), targetRange));
    } catch (const ExceptionBase &e) {
        string errorMsg = "Create merger failed! error:[" + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    CounterSynchronizerPtr counterSynchronizer;
    if (mergeStep.first == MS_DO_MERGE && counterConfig) {
        counterSynchronizer.reset(CounterSynchronizerCreator::create(
                    merger->GetCounterMap(), counterConfig));
        
        if (!counterSynchronizer) {
            BS_LOG(ERROR, "init counterSynchronizer failed");
        } else {
            if (!counterSynchronizer->beginSync()) {
                BS_LOG(ERROR, "create sync counter thread failed");
            }
        }
    }

    if (!doMerge(merger, mergeStep, optimize, partitionId, targetIndexPath)) {
        return false;
    }
    if (mergeStep.first == MS_END_MERGE) {
        for (size_t i = 0; i < mergePartPaths.size(); ++i) {
            _uselessPaths.push_back(mergePartPaths[i]);
        }
    }
    return true;
}

void MergeTask::cleanUselessResource()
{
    for (size_t i = 0; i < _uselessPaths.size(); ++i) {
        if (FileUtil::remove(_uselessPaths[i])) {
            BS_LOG(INFO, "successfully remove dir[%s]",
                   _uselessPaths[i].c_str());
        } else {
            string errorMsg = "failed to remove dir["
                + _uselessPaths[i] + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
        }
    }
}

bool MergeTask::mergeSinglePart(const PartitionId &partitionId,
                                const string &targetIndexPath,
                                const IndexPartitionOptions &options,
                                const pair<MergeStep, uint32_t> mergeStep,
                                bool optimize,
                                const CounterConfigPtr &counterConfig)
{
    IndexPartitionMergerPtr merger = createSinglePartMerger(partitionId, targetIndexPath, options);
    if (!merger) {
        return false;
    }

    CounterSynchronizerPtr counterSynchronizer;
    if (mergeStep.first == MS_DO_MERGE && counterConfig) {
        counterSynchronizer.reset(CounterSynchronizerCreator::create(
                    merger->GetCounterMap(), counterConfig));
        
        if (!counterSynchronizer) {
            BS_LOG(ERROR, "init counterSynchronizer failed");
        } else {
            if (!counterSynchronizer->beginSync()) {
                BS_LOG(ERROR, "create sync counter thread failed");
            }
        }
    }

    if (!doMerge(merger, mergeStep, optimize, partitionId, targetIndexPath)) {
        string errorMsg = "merge failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    VersionCommitter::CleanVersions(targetIndexPath,
                                    options.GetBuildConfig().keepVersionCount,
                                    options.GetReservedVersions());
    return true;
}

IndexPartitionMergerPtr MergeTask::createSinglePartMerger(
    const PartitionId &partitionId,    
    const string &targetIndexPath,
    const IndexPartitionOptions &options)
{
    int32_t workerPathVersion = -1;
    BuildStep buildStep = BUILD_STEP_FULL;
    if (!getWorkerPathVersion(workerPathVersion))
    {
        return IndexPartitionMergerPtr();
    }
    if (!getBuildStep(buildStep))
    {
        return IndexPartitionMergerPtr();
    }

    const string& clusterName = _jobConfig.getClusterName();
    IndexPartitionSchemaPtr schema = _resourceReader->getSchema(clusterName);
    assert(schema);
    IE_NAMESPACE(PartitionRange) targetRange(partitionId.range().from(), partitionId.range().to());    
    try {
        IndexPartitionMergerPtr merger;
        if (buildStep == BUILD_STEP_FULL ||
            workerPathVersion < 0) {
            merger.reset((IndexPartitionMerger*)PartitionMergerCreator::
                         CreateSinglePartitionMerger(targetIndexPath, options,
                                                     _metricProvider, _resourceReader->getPluginPath(), targetRange));
        }
        else {
            merger = CreateIncParallelPartitionMerger(
                partitionId, targetIndexPath, options, workerPathVersion);
        }
        return merger;
    } catch (const ExceptionBase &e) {
        string errorMsg = "create merger for merge failed! exception["
                          + string(e.what()) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return IndexPartitionMergerPtr();
    }
}

bool MergeTask::isMultiPartMerge() const {
    string indexRoot = getValueFromKeyValueMap(_kvMap, INDEX_ROOT_PATH);
    string builderIndexRoot = getValueFromKeyValueMap(_kvMap, BUILDER_INDEX_ROOT_PATH, "");
    if (!builderIndexRoot.empty() && indexRoot != builderIndexRoot) {
        return true;
    }
    const BuildRuleConfig &buildRuleConf =
        _jobConfig.getBuildRuleConf();
    uint32_t originBuildParallelNum = buildRuleConf.buildParallelNum / buildRuleConf.partitionSplitNum;
    assert(originBuildParallelNum * buildRuleConf.partitionSplitNum == buildRuleConf.buildParallelNum);
    return originBuildParallelNum != 1;
}

pair<MergeTask::MergeStep, uint32_t> MergeTask::getMergeStep(
        Mode mode, uint32_t instanceId) const
{
    const BuildRuleConfig &buildRuleConf =
        _jobConfig.getBuildRuleConf();
    // reinit instanceId here
    if (mode == MERGE_MAP) {
        return make_pair(MS_INIT_MERGE, uint32_t(0));
    } else if (mode == MERGE_REDUCE) {
        uint32_t mergeReduceInstanceId = instanceId % buildRuleConf.mergeParallelNum;
        return make_pair(MS_DO_MERGE, mergeReduceInstanceId);
    } else if (mode == END_MERGE_MAP) {
        return make_pair(MS_END_MERGE, uint32_t(0));
    } else {
        return make_pair(MS_DO_NOTHING, uint32_t(0));
    }
}

string MergeTask::timeDiffToString(int64_t timeDiff) {
    stringstream ss;
    ss << timeDiff / 3600 << " h "
       << (timeDiff % 3600) / 60 << " m " << (timeDiff % 60) << " s ";
    return ss.str();
}

void MergeTask::removeObsoleteMergeMeta(const string &mergeMetaRoot, uint32_t reservedVersionNum) {
    vector<string> fileList;
    if (!FileUtil::listDir(mergeMetaRoot, fileList)) {
        BS_LOG(INFO, "list mergeMetaRoot[%s] failed", mergeMetaRoot.c_str());
        return;
    }
    if (fileList.size() <= reservedVersionNum) {
        return;
    }
    sort(fileList.begin(), fileList.end());
    for (size_t i = 0; i < fileList.size() - reservedVersionNum; i++) {
        if (!FileUtil::remove(FileUtil::joinFilePath(mergeMetaRoot, fileList[i]))) {
            BS_LOG(WARN, "remove mergeMetaRoot[%s] failed", fileList[i].c_str());
        }
    }
}

bool MergeTask::doMerge(IndexPartitionMergerPtr &indexPartMerger,
                        const pair<MergeStep, uint32_t> &mergeStep,
                        bool optimize, const PartitionId &partitionId,
                        const string& targetIndexPath)
{
    const BuildRuleConfig &buildRuleConf =
        _jobConfig.getBuildRuleConf();
    uint32_t totalInstanceCount = buildRuleConf.mergeParallelNum;
    string mergeMetaRoot = indexPartMerger->GetMergeMetaDir();
    
    if (_kvMap.find(MERGE_TIMESTAMP) == _kvMap.end()) {
        BS_LOG(ERROR, "Can not find MERGE_TIMESTAMP in kvMap!");
        return false;
    }
    string mergeMetaVersionDir = 
        FileUtil::joinFilePath(mergeMetaRoot, _kvMap[MERGE_TIMESTAMP]);

    BS_LOG(INFO, "mergeMetaVersionDir: %s", mergeMetaVersionDir.c_str());
    string roleId;
    ProtoUtil::partitionIdToRoleId(partitionId, roleId);
    try {
        switch(mergeStep.first)
        {
        case MS_INIT_MERGE:
        {
            int64_t curTs = TimeUtility::currentTimeInMicroSeconds();
            BS_LOG(INFO, "CreateMergeMeta with current ts [%ld]", curTs);
            indexPartMerger->PrepareMerge(curTs);
            MergeMetaPtr mergeMeta = indexPartMerger->CreateMergeMeta(optimize,
                    totalInstanceCount, curTs);
            mergeMeta->Store(mergeMetaVersionDir);
            _mergeStatus.isMergeFinished = true;
            versionid_t targetVersionId = mergeMeta->GetTargetVersion().GetVersionId();
            if (targetVersionId == INVALID_VERSION) {
                Version onDiskVersion;
                VersionLoader::GetVersion(targetIndexPath, onDiskVersion, INVALID_VERSION);
                targetVersionId = onDiskVersion.GetVersionId();
            }
            _mergeStatus.targetVersionId = targetVersionId;
            break;
        }
        case MS_DO_MERGE:
        {
            MergeMetaPtr mergeMeta = indexPartMerger->LoadMergeMeta(mergeMetaVersionDir, false);
            if (!mergeMeta) {
                string errorMsg = "Load merge meta file: ["
                    + mergeMetaVersionDir + "] FAILED.";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;                
            }
            indexPartMerger->DoMerge(optimize, mergeMeta, mergeStep.second);
            break;
        }
        case MS_END_MERGE:
        {
            MergeMetaPtr mergeMeta = indexPartMerger->LoadMergeMeta(mergeMetaVersionDir, true);
            if (!mergeMeta) {
                string errorMsg = "Load merge meta file: ["
                    + mergeMetaVersionDir + "] FAILED.";
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            string alignedVersionId = getValueFromKeyValueMap(_kvMap, ALIGNED_VERSION_ID, "-1");
            int32_t versionId = INVALID_VERSION;
            if (!StringUtil::numberFromString(alignedVersionId, versionId)) {
                BS_LOG(ERROR, "invalid aligned version id: %s", alignedVersionId.c_str());
                return false;
            }
            
            indexPartMerger->EndMerge(mergeMeta, versionId);
            removeObsoleteMergeMeta(mergeMetaRoot);
            //TODO: change target version id
            _mergeStatus.isMergeFinished = true;
            _mergeStatus.targetVersionId = versionId;
            break;
        }
        default:
            break;
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

IndexPartitionMergerPtr MergeTask::CreateIncParallelPartitionMerger(
    const proto::PartitionId &partitionId,    
    const string& targetIndexPath,
    const IndexPartitionOptions& options, uint32_t workerPathVersion)
{
    ParallelBuildInfo incBuildInfo;
    incBuildInfo.batchId = workerPathVersion;
    uint32_t parallelNum = 0;
    if (!getIncBuildParallelNum(parallelNum))
    {
        return IndexPartitionMergerPtr();
    }
    incBuildInfo.parallelNum = parallelNum;
    // merger do not care instanceId
    incBuildInfo.instanceId = 0;
    IE_NAMESPACE(PartitionRange) targetRange(partitionId.range().from(), partitionId.range().to());
    IndexPartitionMergerPtr merger(
            (IndexPartitionMerger*)PartitionMergerCreator::
            CreateIncParallelPartitionMerger(targetIndexPath, incBuildInfo,
                                             options, _metricProvider, _resourceReader->getPluginPath(), targetRange));
    return merger;
}


}
}
