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
#pragma once

#include <limits>
#include <memory>

#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

AUTIL_DECLARE_CLASS_SHARED_PTR(kmonitor, MetricsReporter);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::partition, IndexPartition);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::partition, IndexPartitionResource);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::partition, PartitionGroupResource);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::config, IndexPartitionOptions);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::util, TaskScheduler);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::util, MetricProvider);
AUTIL_DECLARE_CLASS_SHARED_PTR(indexlib::file_system, FileBlockCacheContainer);
AUTIL_DECLARE_STRUCT_SHARED_PTR(indexlib::index_base, CommonBranchHinterOption);

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace indexlib { namespace document {
class SrcSignature;
}} // namespace indexlib::document

namespace indexlib { namespace partition {

class IndexPartitionCreator
{
public:
    explicit IndexPartitionCreator();                   // empty init
    explicit IndexPartitionCreator(int64_t totalQuota); // minimal init
    explicit IndexPartitionCreator(const IndexPartitionResource& indexPartitionResource);
    explicit IndexPartitionCreator(const PartitionGroupResource& partitionGroupResource,
                                   const std::string& partitionName);
    ~IndexPartitionCreator();

public:
    // optional parameters
    IndexPartitionCreator& SetMetricProvider(const util::MetricProviderPtr& metricProvider);
    // SetMetricsReporter just is variant of SetMetricProvider
    IndexPartitionCreator& SetMetricsReporter(const kmonitor::MetricsReporterPtr& metricsReporter);
    IndexPartitionCreator&
    SetMemoryQuotaController(const std::shared_ptr<indexlibv2::MemoryQuotaController>& memoryQuotaController);
    IndexPartitionCreator&
    SetRealtimeQuotaController(const std::shared_ptr<indexlibv2::MemoryQuotaController>& realtimeQuotaController);
    IndexPartitionCreator& SetTaskScheduler(const util::TaskSchedulerPtr& taskScheduler);
    IndexPartitionCreator& SetPartitionGroupName(const std::string& partitionGroupName);
    IndexPartitionCreator& SetPartitionName(const std::string& partitionName);
    IndexPartitionCreator& SetIndexPluginPath(const std::string& indexPluginPath);
    IndexPartitionCreator& SetSrcSignature(const document::SrcSignature& srcSignature);
    IndexPartitionCreator& SetBranchHinterOption(const index_base::CommonBranchHinterOption& branchOption);

public:
    IndexPartitionPtr CreateNormal(const config::IndexPartitionOptions& options); // "normal"
    IndexPartitionPtr CreateCustom(const config::IndexPartitionOptions& options); // "customized"
    // for suez
    IndexPartitionPtr CreateByLoadSchema(const config::IndexPartitionOptions& options, const std::string& schemaRoot,
                                         versionid_t versionId);

private:
    bool CheckIndexPartitionResource();

private:
    std::unique_ptr<IndexPartitionResource> mIndexPartitionResource;

private:
    IE_LOG_DECLARE();
};
DEFINE_SHARED_PTR(IndexPartitionCreator);

}} // namespace indexlib::partition
