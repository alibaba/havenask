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
#ifndef ISEARCH_BS_BUILDERCREATOR_H
#define ISEARCH_BS_BUILDERCREATOR_H

#include "build_service/builder/Builder.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuilderClusterConfig.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "fslib/fslib.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/memory_control/QuotaControl.h"

namespace indexlib { namespace util {
class MetricProvider;
class Metric;
typedef std::shared_ptr<MetricProvider> MetricProviderPtr;
typedef std::shared_ptr<Metric> MetricPtr;
}} // namespace indexlib::util

namespace build_service { namespace builder {

class BuilderCreator
{
public:
    BuilderCreator(const indexlib::partition::IndexPartitionPtr& indexPart = indexlib::partition::IndexPartitionPtr());
    virtual ~BuilderCreator();

private:
    BuilderCreator(const BuilderCreator&);
    BuilderCreator& operator=(const BuilderCreator&);

public:
    virtual Builder* create(const config::ResourceReaderPtr& resourceReader, const KeyValueMap& kvMaps,
                            const proto::PartitionId& partitionId,
                            indexlib::util::MetricProviderPtr metricProvider = indexlib::util::MetricProviderPtr());

private:
    Builder* doCreateIndexlibBuilder(const config::ResourceReaderPtr& resourceReader,
                                     const config::BuilderClusterConfig& clusterConfig,
                                     indexlib::util::MetricProviderPtr metricProvider, fslib::fs::FileLock* fileLock,
                                     const indexlib::partition::BuilderBranchHinter::Option& branchOption);

    Builder* doCreateBuilder(const indexlib::partition::IndexBuilderPtr& indexBuilder,
                             const config::BuilderConfig& builderConfig, int64_t buildTotalMem,
                             indexlib::util::MetricProviderPtr metricProvider, fslib::fs::FileLock* fileLock);
    bool lockPartition(const KeyValueMap& kvMap, const proto::PartitionId& partitionId,
                       std::unique_ptr<fslib::fs::FileLock>& fileLock);
    indexlib::util::QuotaControlPtr createMemoryQuotaControl(int64_t buildTotalMem,
                                                             const config::BuilderConfig& builderConfig) const;
    int64_t getBuildTotalMemory(const config::BuilderClusterConfig& clusterConfig) const;

private:
    static fslib::fs::FileLock* createPartitionLock(const std::string& zkRoot, const proto::PartitionId& pid);
    static bool checkAndStorePartitionMeta(const std::string& indexPath,
                                           const indexlibv2::config::SortDescriptions& sortDescs,
                                           indexlib::file_system::FenceContext* fenceContext);

private:
    // virtual for test
    virtual indexlib::partition::IndexBuilderPtr
    createIndexBuilder(const config::ResourceReaderPtr& resourceReader,
                       const config::BuilderClusterConfig& clusterConfig,
                       const indexlib::util::QuotaControlPtr& quotaControl,
                       const indexlib::partition::BuilderBranchHinter::Option& branchOption,
                       indexlib::util::MetricProviderPtr metricProvider = indexlib::util::MetricProviderPtr());
    virtual Builder* createSortedBuilder(const indexlib::partition::IndexBuilderPtr& indexBuilder,
                                         int64_t buildTotalMem, fslib::fs::FileLock* fileLock);
    virtual Builder* createNormalBuilder(const indexlib::partition::IndexBuilderPtr& indexBuilder,
                                         fslib::fs::FileLock* fileLock);
    virtual Builder* createAsyncBuilder(const indexlib::partition::IndexBuilderPtr& indexBuilder,
                                        fslib::fs::FileLock* fileLock);
    Builder* createLineDataBuilder(const indexlib::config::IndexPartitionSchemaPtr& schema,
                                   const config::BuilderConfig& builderConfig,
                                   indexlib::util::MetricProviderPtr metricProvider, fslib::fs::FileLock* fileLock,
                                   const std::string& epochId);
    virtual bool initBuilder(Builder* builder, const config::BuilderConfig& builderConfig,
                             indexlib::util::MetricProviderPtr metricProvider);

    indexlib::partition::IndexBuilderPtr createIndexBuilderForIncBuild(
        const config::ResourceReaderPtr& resourceReader, const indexlib::config::IndexPartitionSchemaPtr& schema,
        const config::BuilderClusterConfig& clusterConfig, const indexlib::util::QuotaControlPtr& quotaControl,
        indexlib::util::MetricProviderPtr metricProvider,
        const indexlib::partition::BuilderBranchHinter::Option& branchOption);

    bool GetIncBuildParam(const config::ResourceReaderPtr& resourceReader,
                          const config::BuilderClusterConfig& clusterConfig, proto::Range& partitionRange,
                          indexlib::index_base::ParallelBuildInfo& incBuildInfo);

private:
    indexlib::partition::IndexPartitionPtr _indexPart;
    proto::PartitionId _partitionId;
    int32_t _workerPathVersion;
    std::string _indexRootPath;

private:
    static const uint32_t LOCK_TIME_OUT = 60;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuilderCreator);

}} // namespace build_service::builder

#endif // ISEARCH_BS_BUILDERCREATOR_H
