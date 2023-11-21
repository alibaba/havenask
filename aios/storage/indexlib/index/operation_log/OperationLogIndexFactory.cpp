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
#include "indexlib/index/operation_log/OperationLogIndexFactory.h"

#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/IndexReaderParameter.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/operation_log/Common.h"
#include "indexlib/index/operation_log/CompressOperationLogMemIndexer.h"
#include "indexlib/index/operation_log/OperationLogConfig.h"
#include "indexlib/index/operation_log/OperationLogDiskIndexer.h"
#include "indexlib/index/operation_log/OperationLogIndexReader.h"
#include "indexlib/index/operation_log/OperationLogMemIndexer.h"
#include "indexlib/index/operation_log/OperationLogMetrics.h"

namespace indexlib::index {
namespace {
using indexlibv2::config::IIndexConfig;
using indexlibv2::index::DiskIndexerParameter;
using indexlibv2::index::IDiskIndexer;
using indexlibv2::index::IIndexMerger;
using indexlibv2::index::IIndexReader;
using indexlibv2::index::IMemIndexer;
} // namespace

AUTIL_LOG_SETUP(indexlib.index, OperationLogIndexFactory);

OperationLogIndexFactory::OperationLogIndexFactory() {}

OperationLogIndexFactory::~OperationLogIndexFactory() {}

std::shared_ptr<IDiskIndexer>
OperationLogIndexFactory::CreateDiskIndexer(const std::shared_ptr<IIndexConfig>& indexConfig,
                                            const DiskIndexerParameter& indexerParam) const
{
    return std::make_shared<OperationLogDiskIndexer>(indexerParam.segmentId);
}

std::shared_ptr<IMemIndexer>
OperationLogIndexFactory::CreateMemIndexer(const std::shared_ptr<IIndexConfig>& indexConfig,
                                           const indexlibv2::index::MemIndexerParameter& indexerParam) const
{
    segmentid_t segmentid = indexerParam.segmentId;
    std::string identifier = "__operation_log_metrics_identifier_" + std::to_string(segmentid);
    std::shared_ptr<OperationLogMemIndexer> indexer;
    assert(indexConfig);
    auto opConfig = std::dynamic_pointer_cast<OperationLogConfig>(indexConfig);
    assert(opConfig);
    if (opConfig->IsCompress()) {
        indexer = std::make_shared<CompressOperationLogMemIndexer>(segmentid);
    } else {
        indexer = std::make_shared<OperationLogMemIndexer>(segmentid);
    }
    if (indexerParam.metricsManager) {
        std::shared_ptr<OperationLogMetrics> operationLogMetrics =
            std::dynamic_pointer_cast<OperationLogMetrics>(indexerParam.metricsManager->CreateMetrics(
                identifier, [&indexerParam, segmentid, indexer]() -> std::shared_ptr<indexlibv2::framework::IMetrics> {
                    return std::make_shared<OperationLogMetrics>(
                        segmentid, indexerParam.metricsManager->GetMetricsReporter(),
                        std::bind(&OperationLogMemIndexer::GetTotalOperationCount,
                                  dynamic_cast<OperationLogMemIndexer*>(indexer.get())));
                }));
        indexer->RegisterMetrics(operationLogMetrics);
    }
    return indexer;
}

std::unique_ptr<IIndexReader>
OperationLogIndexFactory::CreateIndexReader(const std::shared_ptr<IIndexConfig>& indexConfig,
                                            const indexlibv2::index::IndexReaderParameter&) const
{
    return std::make_unique<OperationLogIndexReader>();
}

class OperationLogMerger : public IIndexMerger
{
public:
    OperationLogMerger() : IIndexMerger() {}
    ~OperationLogMerger() {}
    Status Init(const std::shared_ptr<IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override
    {
        return Status::OK();
    }
    Status Merge(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<indexlibv2::framework::IndexTaskResourceManager>& taskResourceManager) override
    {
        return Status::OK();
    }
};

std::unique_ptr<IIndexMerger>
OperationLogIndexFactory::CreateIndexMerger(const std::shared_ptr<IIndexConfig>& indexConfig) const
{
    return std::unique_ptr<IIndexMerger>(new OperationLogMerger);
}

std::unique_ptr<indexlibv2::config::IIndexConfig>
OperationLogIndexFactory::CreateIndexConfig(const autil::legacy::Any& any) const
{
    assert(false);
    return nullptr;
}

std::string OperationLogIndexFactory::GetIndexPath() const { return OPERATION_LOG_INDEX_PATH; }
REGISTER_INDEX_FACTORY(operation_log, OperationLogIndexFactory);

} // namespace indexlib::index
