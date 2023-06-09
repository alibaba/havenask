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
#include "build_service/builder/BuilderCreator.h"

#include "autil/StringUtil.h"
#include "build_service/builder/AsyncBuilder.h"
#include "build_service/builder/LineDataBuilder.h"
#include "build_service/builder/SortedBuilder.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/IndexPartitionOptionsWrapper.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_partition.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::util;

using namespace build_service::util;
using namespace build_service::proto;
using namespace build_service::config;
using namespace build_service::common;
using namespace indexlib;

namespace build_service { namespace builder {

BS_LOG_SETUP(builder, BuilderCreator);

BuilderCreator::BuilderCreator(const indexlib::partition::IndexPartitionPtr& indexPart)
    : _indexPart(indexPart)
    , _workerPathVersion(-1)
{
}

BuilderCreator::~BuilderCreator() {}

Builder* BuilderCreator::create(const ResourceReaderPtr& resourceReader, const KeyValueMap& kvMap,
                                const PartitionId& partitionId, indexlib::util::MetricProviderPtr metricProvider)
{
    assert(partitionId.clusternames_size() == 1);

    unique_ptr<fslib::fs::FileLock> fileLock;
    if (!lockPartition(kvMap, partitionId, fileLock)) {
        return nullptr;
    }
    _partitionId = partitionId;
    // indicate whether it's a backup worker or not.
    uint32_t backupId = 0;
    if (partitionId.has_backupid() && partitionId.backupid() != 0) {
        backupId = partitionId.backupid();
        BS_LOG(INFO, "backupId [%d], starting", backupId);
    }
    const string& clusterName = partitionId.clusternames(0);
    string mergeConfigName = getValueFromKeyValueMap(kvMap, MERGE_CONFIG_NAME);

    BuilderClusterConfig clusterConfig;
    if (!clusterConfig.init(clusterName, *resourceReader, mergeConfigName)) {
        BS_LOG(ERROR, "failed to init BuilderClusterConfig");
        return nullptr;
    }

    if (_indexPart) {
        if (backupId) {
            BS_LOG(ERROR, "can't create backup buidler with legacy indexPart!");
            return nullptr;
        }
        // update indexOptions by indexPartition
        // fix job mode realtime buildConfig effective less problem
        clusterConfig.indexOptions = _indexPart->GetOptions();
    }

    if (!IndexPartitionOptionsWrapper::initReservedVersions(kvMap, clusterConfig.indexOptions)) {
        return nullptr;
    }

    IndexPartitionOptionsWrapper::initOperationIds(kvMap, clusterConfig.indexOptions);
    IndexPartitionOptionsWrapper::initSchemaUpdateStandard(kvMap, clusterConfig.indexOptions);

    string workerPathVersionStr = getValueFromKeyValueMap(kvMap, WORKER_PATH_VERSION, "");
    if (workerPathVersionStr.empty()) {
        _workerPathVersion = -1;
    } else if (!StringUtil::fromString(workerPathVersionStr, _workerPathVersion)) {
        BS_LOG(ERROR, "invalid workerPathVersion [%s]", workerPathVersionStr.c_str());
        return nullptr;
    }

    _indexRootPath = getValueFromKeyValueMap(kvMap, INDEX_ROOT_PATH);
    if (_indexRootPath.empty() && !_indexPart) {
        BS_LOG(ERROR, "index root path can not be empty");
        return nullptr;
    }

    string epochId = getValueFromKeyValueMap(kvMap, BUILDER_EPOCH_ID, "");
    bool branchLegacy = _indexPart || getValueFromKeyValueMap(kvMap, BUILDER_BRANCH_NAME_LEGACY, "") == "true";
    auto branchOption = branchLegacy ? CommonBranchHinterOption::Legacy(backupId, epochId)
                                     : CommonBranchHinterOption::Normal(backupId, epochId);
    if (!_indexPart) {
        auto schema = resourceReader->getSchema(clusterConfig.clusterName);
        if (!schema) {
            return nullptr;
        }
        if (tt_linedata == schema->GetTableType()) {
            if (backupId) {
                BS_LOG(ERROR, "linedata builder not support backup node strategy!");
                return nullptr;
            }
            return createLineDataBuilder(schema, clusterConfig.builderConfig, metricProvider, fileLock.release(),
                                         epochId);
        }
    }
    return doCreateIndexlibBuilder(resourceReader, clusterConfig, metricProvider, fileLock.release(), branchOption);
}

Builder* BuilderCreator::doCreateIndexlibBuilder(const ResourceReaderPtr& resourceReader,
                                                 const BuilderClusterConfig& clusterConfig,
                                                 indexlib::util::MetricProviderPtr metricProvider,
                                                 fslib::fs::FileLock* fileLock,
                                                 const BuilderBranchHinter::Option& branchOption)
{
    int64_t buildTotalMem = getBuildTotalMemory(clusterConfig);
    QuotaControlPtr quotaControl = createMemoryQuotaControl(buildTotalMem, clusterConfig.builderConfig);
    if (!quotaControl) {
        return nullptr;
    }

    IndexBuilderPtr indexBuilder =
        createIndexBuilder(resourceReader, clusterConfig, quotaControl, branchOption, metricProvider);
    if (!indexBuilder) {
        BS_LOG(ERROR, "failed to create IndexBuilder");
        return nullptr;
    }

    return doCreateBuilder(indexBuilder, clusterConfig.builderConfig, buildTotalMem, metricProvider, fileLock);
}

IndexBuilderPtr BuilderCreator::createIndexBuilder(const ResourceReaderPtr& resourceReader,
                                                   const BuilderClusterConfig& clusterConfig,
                                                   const QuotaControlPtr& quotaControl,
                                                   const BuilderBranchHinter::Option& branchOption,
                                                   indexlib::util::MetricProviderPtr metricProvider)

{
    IndexBuilderPtr indexBuilder;
    indexlib::PartitionRange partitionRange(_partitionId.range().from(), _partitionId.range().to());
    if (_indexPart) {
        assert(branchOption.epochId == "");
        // realtime mode : use same index part object with search.
        indexBuilder.reset(new IndexBuilder(_indexPart, quotaControl, metricProvider, partitionRange));
    } else {
        // normal mode.
        IndexPartitionSchemaPtr schema = resourceReader->getSchema(clusterConfig.clusterName);
        if (!schema) {
            string errorMsg = "Invalid schema[" + clusterConfig.tableName + "] to build";
            BS_LOG(WARN, "%s", errorMsg.c_str());
            return IndexBuilderPtr();
        }
        if (clusterConfig.overwriteSchemaName) {
            BS_LOG(INFO, "overwrite schemaName[%s] to clusterName[%s], tableName[%s]", schema->GetSchemaName().c_str(),
                   clusterConfig.clusterName.c_str(), clusterConfig.tableName.c_str());
            schema->SetSchemaName(clusterConfig.clusterName);
        }
        // TODO: remove sortBuild true/false in config
        if (_partitionId.step() == BUILD_STEP_FULL) {
            string indexPath = IndexPathConstructor::constructIndexPath(_indexRootPath, _partitionId);
            FenceContextPtr fenceContext(FslibWrapper::CreateFenceContext(indexPath, branchOption.epochId));
            indexlibv2::config::SortDescriptions sortDescs;
            if (clusterConfig.builderConfig.sortBuild) {
                sortDescs = clusterConfig.builderConfig.sortDescriptions;
            }
            if (!checkAndStorePartitionMeta(indexPath, sortDescs, fenceContext.get())) {
                string errorMsg = "check and store partition meta failed";
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return IndexBuilderPtr();
            }
            IndexPartitionOptions indexPartitonOptions(clusterConfig.indexOptions);
            indexPartitonOptions.GetBuildConfig(false).buildPhase = BuildConfig::BP_FULL;
            indexBuilder.reset(new IndexBuilder(indexPath, indexPartitonOptions, schema, quotaControl, branchOption,
                                                metricProvider, resourceReader->getPluginPath(), partitionRange));
        } else {
            indexBuilder = createIndexBuilderForIncBuild(resourceReader, schema, clusterConfig, quotaControl,
                                                         metricProvider, branchOption);
            if (!indexBuilder) {
                string errorMsg = "create index builder for inc build fail.";
                BS_LOG(WARN, "%s", errorMsg.c_str());
                return IndexBuilderPtr();
            }
        }
    }
    if (!indexBuilder->Init()) {
        string errorMsg = "init IndexBuilder failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        indexBuilder.reset();
    }
    return indexBuilder;
}

bool BuilderCreator::checkAndStorePartitionMeta(const string& indexPath,
                                                const indexlibv2::config::SortDescriptions& sortDescs,
                                                FenceContext* fenceContext)
{
    try {
        if (!PartitionMeta::IsExist(indexPath)) {
            PartitionMeta meta(sortDescs);
            meta.Store(indexPath, fenceContext);
            return true;
        } else {
            PartitionMeta originalMeta;
            originalMeta.Load(indexPath);
            if (sortDescs != originalMeta.GetSortDescriptions()) {
                stringstream ss;
                ss << "Partition meta already exist[" << ToJsonString(originalMeta.GetSortDescriptions())
                   << "] and don't match with config[" << ToJsonString(sortDescs) << "].";
                string errorMsg = ss.str();
                BS_LOG(ERROR, "%s", errorMsg.c_str());
                return false;
            }
            return true;
        }
    } catch (const FileIOException& e) {
        BS_LOG(ERROR, "%s", e.what());
        return false;
    } catch (const ExceptionBase& e) {
        BS_LOG(ERROR, "%s", e.what());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "%s", "unknown exception");
        return false;
    }
}

bool BuilderCreator::lockPartition(const KeyValueMap& kvMap, const PartitionId& partitionId,
                                   unique_ptr<fslib::fs::FileLock>& fileLock)
{
    string zkRoot = getValueFromKeyValueMap(kvMap, ZOOKEEPER_ROOT);
    if (zkRoot.empty()) {
        return true;
    }
    fileLock.reset(createPartitionLock(zkRoot, partitionId));
    if (!fileLock) {
        string errorMsg = "create partition lock for [" + partitionId.ShortDebugString() + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (fileLock->lock(LOCK_TIME_OUT) != 0) {
        string errorMsg = "lock partition[" + partitionId.ShortDebugString() + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

QuotaControlPtr BuilderCreator::createMemoryQuotaControl(int64_t buildTotalMem,
                                                         const BuilderConfig& builderConfig) const
{
    QuotaControlPtr memQuotaControl(new QuotaControl(buildTotalMem * 1024 * 1024));
    if (_indexPart) {
        // realtime
        return memQuotaControl;
    }
    if (builderConfig.sortBuild) {
        if (!SortedBuilder::reserveMemoryQuota(builderConfig, buildTotalMem, memQuotaControl)) {
            BS_LOG(ERROR, "cannot allocate memory for sort build");
            return QuotaControlPtr();
        }
    } else if (builderConfig.asyncBuild && builderConfig.asyncQueueMem != BuilderConfig::INVALID_ASYNC_QUEUE_MEM) {
        if (!memQuotaControl->AllocateQuota(builderConfig.asyncQueueMem * 1024 * 1024)) {
            BS_LOG(ERROR, "cannot allocate memory for async build");
            return QuotaControlPtr();
        }
    }
    return memQuotaControl;
}

int64_t BuilderCreator::getBuildTotalMemory(const BuilderClusterConfig& clusterConfig) const
{
    if (_indexPart) {
        return clusterConfig.indexOptions.GetBuildConfig(true).buildTotalMemory;
    }
    return clusterConfig.indexOptions.GetBuildConfig(false).buildTotalMemory;
}

Builder* BuilderCreator::doCreateBuilder(const IndexBuilderPtr& indexBuilder, const BuilderConfig& builderConfig,
                                         int64_t buildTotalMem, indexlib::util::MetricProviderPtr metricProvider,
                                         fslib::fs::FileLock* fileLock)
{
    Builder* builder = NULL;
    if (builderConfig.sortBuild && !_indexPart) {
        builder = createSortedBuilder(indexBuilder, buildTotalMem, fileLock);
    } else if (builderConfig.asyncBuild) {
        // todo: shut down offline support asyncBuild?
        builder = createAsyncBuilder(indexBuilder, fileLock);
    } else {
        builder = createNormalBuilder(indexBuilder, fileLock);
    }
    if (!initBuilder(builder, builderConfig, metricProvider)) {
        DELETE_AND_SET_NULL(builder);
        return NULL;
    }
    return builder;
}

Builder* BuilderCreator::createAsyncBuilder(const IndexBuilderPtr& indexBuilder, fslib::fs::FileLock* fileLock)
{
    return new AsyncBuilder(indexBuilder, fileLock, _partitionId.buildid());
}

Builder* BuilderCreator::createSortedBuilder(const IndexBuilderPtr& indexBuilder, int64_t buildTotalMem,
                                             fslib::fs::FileLock* fileLock)
{
    return new SortedBuilder(indexBuilder, buildTotalMem, fileLock, _partitionId.buildid());
}

Builder* BuilderCreator::createNormalBuilder(const IndexBuilderPtr& indexBuilder, fslib::fs::FileLock* fileLock)
{
    return new Builder(indexBuilder, fileLock, _partitionId.buildid());
}

bool BuilderCreator::initBuilder(Builder* builder, const BuilderConfig& builderConfig,
                                 indexlib::util::MetricProviderPtr metricProvider)
{
    if (_indexPart) {
        builder->enableSpeedLimiter(); // realtime build
    }
    return builder->init(builderConfig, metricProvider);
}

Builder* BuilderCreator::createLineDataBuilder(const IndexPartitionSchemaPtr& schema,
                                               const BuilderConfig& builderConfig,
                                               indexlib::util::MetricProviderPtr metricProvider,
                                               fslib::fs::FileLock* fileLock, const string& epochId)
{
    string indexPath = IndexPathConstructor::constructIndexPath(_indexRootPath, _partitionId);
    auto builder = new LineDataBuilder(indexPath, schema, fileLock, epochId);
    if (!initBuilder(builder, builderConfig, metricProvider)) {
        return NULL;
    }
    return builder;
}

fslib::fs::FileLock* BuilderCreator::createPartitionLock(const string& zkRoot, const PartitionId& pid)
{
    string lockPath = PathDefine::getIndexPartitionLockPath(zkRoot, pid);
    return fslib::fs::FileSystem::getFileLock(lockPath);
}

IndexBuilderPtr BuilderCreator::createIndexBuilderForIncBuild(const ResourceReaderPtr& resourceReader,
                                                              const IndexPartitionSchemaPtr& schema,
                                                              const BuilderClusterConfig& clusterConfig,
                                                              const QuotaControlPtr& quotaControl,
                                                              indexlib::util::MetricProviderPtr metricProvider,
                                                              const BuilderBranchHinter::Option& branchOption)
{
    ParallelBuildInfo incBuildInfo;
    proto::Range partRange;
    if (!GetIncBuildParam(resourceReader, clusterConfig, partRange, incBuildInfo)) {
        return IndexBuilderPtr();
    }
    indexlib::PartitionRange indexPartRange(_partitionId.range().from(), _partitionId.range().to());
    // get partition path
    proto::PartitionId parentPartId = _partitionId;
    *parentPartId.mutable_range() = partRange;
    string indexPath = IndexPathConstructor::constructIndexPath(_indexRootPath, parentPartId);

    // check partition meta
    indexlibv2::config::SortDescriptions sortDescs;
    if (clusterConfig.builderConfig.sortBuild) {
        sortDescs = clusterConfig.builderConfig.sortDescriptions;
    }
    FenceContextPtr fenceContext(FslibWrapper::CreateFenceContext(indexPath, branchOption.epochId));
    if (!checkAndStorePartitionMeta(indexPath, sortDescs, fenceContext.get())) {
        string errorMsg = "check and store partition meta failed";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return IndexBuilderPtr();
    }

    IndexBuilderPtr indexBuilder;
    IndexPartitionOptions indexPartitonOptions(clusterConfig.indexOptions);
    indexPartitonOptions.GetBuildConfig(false).buildPhase = BuildConfig::BP_INC;
    if (_workerPathVersion < 0) {
        // old version admin  do not support inc parallel build
        indexlib::PartitionRange parentRange(parentPartId.range().from(), parentPartId.range().to());
        indexBuilder.reset(new IndexBuilder(indexPath, indexPartitonOptions, schema, quotaControl, branchOption,
                                            metricProvider, resourceReader->getPluginPath(), parentRange));
    } else {
        indexBuilder.reset(new IndexBuilder(indexPath, incBuildInfo, indexPartitonOptions, schema, quotaControl,
                                            branchOption, metricProvider, resourceReader->getPluginPath(),
                                            indexPartRange));
    }
    return indexBuilder;
}

bool BuilderCreator::GetIncBuildParam(const config::ResourceReaderPtr& resourceReader,
                                      const config::BuilderClusterConfig& clusterConfig, proto::Range& partitionRange,
                                      ParallelBuildInfo& incBuildInfo)
{
    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(clusterConfig.clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        string errorMsg = "parse cluster_config.builder_rule_config for [" + clusterConfig.clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    incBuildInfo.batchId = _workerPathVersion;

    uint32_t actualParallelNum = buildRuleConfig.incBuildParallelNum;

    if (_workerPathVersion < 0 && actualParallelNum > 1) {
        IE_LOG(WARN,
               "target workerPathVersion [%d] less than 0, may be target from old version admin."
               "will use single instance for inc build",
               _workerPathVersion);
        actualParallelNum = 1;
    }

    uint32_t partitionIdx = 0;
    uint32_t instanceId;

    if (!RangeUtil::getParallelInfoByRange(RANGE_MIN, RANGE_MAX, buildRuleConfig.partitionCount, actualParallelNum,
                                           _partitionId.range(), partitionIdx, instanceId)) {
        if (actualParallelNum == 1) {
            BS_LOG(ERROR,
                   "deduce parallel idx from range [%u:%u] fail,"
                   " partitionCount [%u], parallelNum [%u].",
                   _partitionId.range().from(), _partitionId.range().to(), buildRuleConfig.partitionCount,
                   actualParallelNum);
            return false;
        }
        BS_LOG(WARN, "try deduce parallel idx as single instance,"
                     " in case admin treat this builder as an old protocoal version one");
        actualParallelNum = 1;
        if (!RangeUtil::getParallelInfoByRange(RANGE_MIN, RANGE_MAX, buildRuleConfig.partitionCount, actualParallelNum,
                                               _partitionId.range(), partitionIdx, instanceId)) {
            BS_LOG(ERROR,
                   "deduce parallel idx from range [%u:%u] fail,"
                   " partitionCount [%u], parallelNum [%u].",
                   _partitionId.range().from(), _partitionId.range().to(), buildRuleConfig.partitionCount,
                   actualParallelNum);
            return false;
        }
    }

    incBuildInfo.parallelNum = actualParallelNum;
    incBuildInfo.instanceId = instanceId;
    vector<proto::Range> partRanges = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, buildRuleConfig.partitionCount);
    assert(partitionIdx < buildRuleConfig.partitionCount);
    partitionRange = partRanges[partitionIdx];
    return true;
}

}} // namespace build_service::builder
