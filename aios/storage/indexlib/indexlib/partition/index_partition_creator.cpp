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
#include "indexlib/partition/index_partition_creator.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/custom_offline_partition.h"
#include "indexlib/partition/custom_online_partition.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexPartitionCreator);

IndexPartitionCreator::IndexPartitionCreator() : mIndexPartitionResource(new IndexPartitionResource) {}

IndexPartitionCreator::IndexPartitionCreator(int64_t totalQuota) : mIndexPartitionResource(new IndexPartitionResource)
{
    mIndexPartitionResource->memoryQuotaController =
        std::make_shared<indexlibv2::MemoryQuotaController>(/*name*/ "", totalQuota);
    mIndexPartitionResource->taskScheduler = util::TaskScheduler::Create();
}

IndexPartitionCreator::IndexPartitionCreator(const IndexPartitionResource& indexPartitionResource)
    : mIndexPartitionResource(new IndexPartitionResource(indexPartitionResource))
{
}

IndexPartitionCreator::IndexPartitionCreator(const PartitionGroupResource& partitionGroupResource,
                                             const std::string& partitionName)
    : IndexPartitionCreator(IndexPartitionResource(partitionGroupResource, partitionName))
{
}

IndexPartitionCreator::~IndexPartitionCreator() {}

bool IndexPartitionCreator::CheckIndexPartitionResource()
{
    if (!mIndexPartitionResource->memoryQuotaController) {
        IE_LOG(ERROR, "partitionName[%s] memoryQuotaController is empty.",
               mIndexPartitionResource->partitionName.c_str());
        assert(false);
        return false;
    }
    if (!mIndexPartitionResource->taskScheduler) {
        IE_LOG(ERROR, "partitionName[%s] taskScheduler is empty.", mIndexPartitionResource->partitionName.c_str());
        assert(false);
        return false;
    }
    return true;
}

IndexPartitionPtr IndexPartitionCreator::CreateByLoadSchema(const config::IndexPartitionOptions& indexPartitionOptions,
                                                            const std::string& schemaRoot, versionid_t versionId)
{
    if (!CheckIndexPartitionResource()) {
        return IndexPartitionPtr();
    }
    try {
        auto schema = index_base::SchemaAdapter::LoadSchemaByVersionId(schemaRoot, versionId);
        if (!schema) {
            IE_LOG(ERROR, "load schema from [%s] failed", schemaRoot.c_str());
            return IndexPartitionPtr();
        }

        if (schema->GetTableType() == tt_customized) {
            return CreateCustom(indexPartitionOptions);
        } else {
            return CreateNormal(indexPartitionOptions);
        }
    } catch (const std::exception& e) {
        IE_LOG(ERROR, "caught exception: %s", e.what());
    } catch (...) {
        IE_LOG(ERROR, "caught unknown exception");
    }
    return IndexPartitionPtr();
}

IndexPartitionPtr IndexPartitionCreator::CreateNormal(const config::IndexPartitionOptions& indexPartitionOptions)
{
    if (!CheckIndexPartitionResource()) {
        return IndexPartitionPtr();
    }
    if (indexPartitionOptions.IsOnline()) {
        return IndexPartitionPtr(new OnlinePartition(*mIndexPartitionResource));
    }
    return IndexPartitionPtr(new OfflinePartition(*mIndexPartitionResource));
}

IndexPartitionPtr IndexPartitionCreator::CreateCustom(const config::IndexPartitionOptions& indexPartitionOptions)
{
    if (!CheckIndexPartitionResource()) {
        return IndexPartitionPtr();
    }
    if (indexPartitionOptions.IsOnline()) {
        return IndexPartitionPtr(new CustomOnlinePartition(*mIndexPartitionResource));
    }
    assert(indexPartitionOptions.IsOffline());
    return IndexPartitionPtr(new CustomOfflinePartition(*mIndexPartitionResource));
}

// Write
IndexPartitionCreator& IndexPartitionCreator::SetMetricProvider(const util::MetricProviderPtr& metricProvider)
{
    mIndexPartitionResource->metricProvider = metricProvider;
    return *this;
}
IndexPartitionCreator& IndexPartitionCreator::SetMetricsReporter(const kmonitor::MetricsReporterPtr& metricsReporter)
{
    return SetMetricProvider(util::MetricProvider::Create(metricsReporter));
}
IndexPartitionCreator& IndexPartitionCreator::SetMemoryQuotaController(
    const std::shared_ptr<indexlibv2::MemoryQuotaController>& memoryQuotaController)
{
    mIndexPartitionResource->memoryQuotaController = memoryQuotaController;
    return *this;
}
IndexPartitionCreator& IndexPartitionCreator::SetRealtimeQuotaController(
    const std::shared_ptr<indexlibv2::MemoryQuotaController>& realtimeQuotaController)
{
    mIndexPartitionResource->realtimeQuotaController = realtimeQuotaController;
    return *this;
}
IndexPartitionCreator& IndexPartitionCreator::SetTaskScheduler(const util::TaskSchedulerPtr& taskScheduler)
{
    mIndexPartitionResource->taskScheduler = taskScheduler;
    return *this;
}
IndexPartitionCreator& IndexPartitionCreator::SetPartitionGroupName(const std::string& partitionGroupName)
{
    mIndexPartitionResource->partitionGroupName = partitionGroupName;
    return *this;
}
IndexPartitionCreator& IndexPartitionCreator::SetPartitionName(const std::string& partitionName)
{
    mIndexPartitionResource->partitionName = partitionName;
    return *this;
}
IndexPartitionCreator& IndexPartitionCreator::SetIndexPluginPath(const std::string& indexPluginPath)
{
    mIndexPartitionResource->indexPluginPath = indexPluginPath;
    return *this;
}
IndexPartitionCreator& IndexPartitionCreator::SetSrcSignature(const document::SrcSignature& srcSignature)
{
    mIndexPartitionResource->srcSignature = srcSignature;
    return *this;
}
IndexPartitionCreator&
IndexPartitionCreator::SetBranchHinterOption(const index_base::CommonBranchHinterOption& branchOption)
{
    mIndexPartitionResource->branchOption = branchOption;
    return *this;
}
}} // namespace indexlib::partition
