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
#include "indexlib/index/deletionmap/DeletionMapIndexFactory.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapDiskIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapIndexReader.h"
#include "indexlib/index/deletionmap/DeletionMapMemIndexer.h"
#include "indexlib/index/deletionmap/DeletionMapMerger.h"
#include "indexlib/index/deletionmap/DeletionMapMetrics.h"
#include "kmonitor/client/core/MetricsTags.h"

using namespace std;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DeletionMapIndexFactory);

DeletionMapIndexFactory::DeletionMapIndexFactory() {}

DeletionMapIndexFactory::~DeletionMapIndexFactory() {}

std::shared_ptr<IDiskIndexer>
DeletionMapIndexFactory::CreateDiskIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                           const IndexerParameter& indexerParam) const
{
    assert(nullptr != indexerParam.metricsManager);
    segmentid_t segmentid = indexerParam.segmentId;
    auto diskIndexer = std::make_shared<index::DeletionMapDiskIndexer>(indexerParam.docCount, segmentid);
    std::string identifier = "__disk_deletionmap_metrics_identifier_" + std::to_string((uint64_t)(diskIndexer.get())) +
                             "_" + std::to_string(segmentid);
    auto tags = make_shared<kmonitor::MetricsTags>("segmentid", std::to_string(segmentid));
    std::string metricName = "partition/segmentDeletedDocCount";
    auto func = std::bind(&DeletionMapDiskIndexer::GetDeletedDocCount, diskIndexer.get());
    std::shared_ptr<DeletionMapMetrics> deletionMapMetrics =
        std::dynamic_pointer_cast<DeletionMapMetrics>(indexerParam.metricsManager->CreateMetrics(
            identifier, [&indexerParam, &metricName, &tags, &func]() -> std::shared_ptr<framework::IMetrics> {
                return std::make_shared<DeletionMapMetrics>(
                    metricName, tags, indexerParam.metricsManager->GetMetricsReporter(), std::move(func));
            }));
    diskIndexer->RegisterMetrics(deletionMapMetrics);
    if (!deletionMapMetrics) {
        AUTIL_LOG(ERROR, "get deletion map metrics with identifier [%s] failed, will not report", identifier.c_str());
    }
    return diskIndexer;
}

std::shared_ptr<IMemIndexer>
DeletionMapIndexFactory::CreateMemIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                          const IndexerParameter& indexerParam) const
{
    segmentid_t segmentid = indexerParam.segmentId;
    auto memIndexer =
        std::make_shared<index::DeletionMapMemIndexer>(segmentid, indexerParam.sortDescriptions.size() > 0);
    if (indexerParam.metricsManager) {
        std::string identifier = "__mem_deletionmap_metrics_identifier_" +
                                 std::to_string((uint64_t)(memIndexer.get())) + "_" + std::to_string(segmentid);
        auto tags = make_shared<kmonitor::MetricsTags>("segmentid", std::to_string(segmentid));
        std::string metricName = "partition/segmentDeletedDocCount";
        auto func = std::bind(&DeletionMapMemIndexer::GetDeletedDocCount, memIndexer.get());
        std::shared_ptr<DeletionMapMetrics> deletionMapMetrics =
            std::dynamic_pointer_cast<DeletionMapMetrics>(indexerParam.metricsManager->CreateMetrics(
                identifier, [&indexerParam, &metricName, &tags, &func]() -> std::shared_ptr<framework::IMetrics> {
                    return std::make_shared<DeletionMapMetrics>(
                        metricName, tags, indexerParam.metricsManager->GetMetricsReporter(), std::move(func));
                }));
        memIndexer->RegisterMetrics(deletionMapMetrics);
        if (!deletionMapMetrics) {
            AUTIL_LOG(ERROR, "get deletion map metrics with identifier [%s] failed, will not report",
                      identifier.c_str());
        }
    }
    return memIndexer;
}

std::unique_ptr<IIndexReader>
DeletionMapIndexFactory::CreateIndexReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                           const IndexerParameter& indexerParam) const
{
    assert(nullptr != indexerParam.metricsManager);
    std::string identifier =
        "__deletionmap_reader_metrics_identifier" + std::to_string(autil::TimeUtility::currentTimeInMicroSeconds());

    auto indexReader = std::make_unique<index::DeletionMapIndexReader>();
    shared_ptr<kmonitor::MetricsTags> tags;
    std::string metricName = "partition/deletedDocCount";
    auto func = std::bind(&DeletionMapIndexReader::GetDeletedDocCount, indexReader.get());
    std::shared_ptr<DeletionMapMetrics> deletionMapMetrics =
        std::dynamic_pointer_cast<DeletionMapMetrics>(indexerParam.metricsManager->CreateMetrics(
            identifier, [&indexerParam, &metricName, &tags, &func]() -> std::shared_ptr<framework::IMetrics> {
                return std::make_shared<DeletionMapMetrics>(
                    metricName, tags, indexerParam.metricsManager->GetMetricsReporter(), std::move(func));
            }));
    indexReader->RegisterMetrics(deletionMapMetrics);
    if (!deletionMapMetrics) {
        AUTIL_LOG(ERROR, "get deletion map metrics with identifier [%s] failed, will not report", identifier.c_str());
    }
    return std::move(indexReader);
}

std::unique_ptr<IIndexMerger>
DeletionMapIndexFactory::CreateIndexMerger(const std::shared_ptr<config::IIndexConfig>& indexConfig) const
{
    return make_unique<DeletionMapMerger>();
}

std::unique_ptr<config::IIndexConfig> DeletionMapIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    assert(false);
    return nullptr;
}

std::string DeletionMapIndexFactory::GetIndexPath() const { return DELETION_MAP_INDEX_PATH; }
REGISTER_INDEX(deletionmap, DeletionMapIndexFactory);
} // namespace indexlibv2::index
