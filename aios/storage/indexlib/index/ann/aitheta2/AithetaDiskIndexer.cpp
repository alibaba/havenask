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
#include "indexlib/index/ann/aitheta2/AithetaDiskIndexer.h"

#include "indexlib/base/Status.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/ann/ANNIndexConfig.h"
#include "indexlib/index/ann/aitheta2/AithetaIndexConfig.h"
#include "indexlib/index/ann/aitheta2/impl/CustomizedAithetaLogger.h"
#include "indexlib/index/ann/aitheta2/util/MetricReporter.h"

using namespace std;
using namespace indexlib;

namespace indexlibv2::index::ann {
AUTIL_LOG_SETUP(indexlib.index, AithetaDiskIndexer);

AithetaDiskIndexer::AithetaDiskIndexer(const DiskIndexerParameter& indexerParam) : _indexerParam(indexerParam) {}

Status AithetaDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    CustomizedAiThetaLogger::RegisterIndexLogger();
    auto annIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::ANNIndexConfig>(indexConfig);
    if (nullptr == annIndexConfig) {
        return Status::InvalidArgs("cast to ANNIndexConfig failed.");
    }
    _indexName = annIndexConfig->GetIndexName();
    _aithetaIndexConfig = AithetaIndexConfig(annIndexConfig->GetParameters());
    auto aithetaIndexDir = indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory)
                               ->GetDirectory(indexConfig->GetIndexName(), false);
    if (nullptr == aithetaIndexDir) {
        RETURN_STATUS_ERROR(InternalError, "fail to get aithetaIndexDir, indexName[%s] indexType[%s]",
                            _indexName.c_str(), indexConfig->GetIndexType().c_str());
    }

    if (!SegmentMeta::IsExist(aithetaIndexDir)) {
        AUTIL_LOG(INFO, "directory[%s] is empty", aithetaIndexDir->DebugString().c_str());
        return Status::OK();
    }

    _normalSegment = std::make_shared<NormalSegment>(aithetaIndexDir, _aithetaIndexConfig, true);
    if (!_normalSegment->Open()) {
        return Status::InternalError("normal segment open failed.");
    }
    _currentMemUsed = EstimateMemUsed(indexConfig, indexDirectory);
    return Status::OK();
}

std::pair<Status, std::shared_ptr<NormalSegmentSearcher>>
AithetaDiskIndexer::CreateSearcher(const AithetaIndexConfig& segmentSearchConfig, docid_t segmentBaseDocId,
                                   const std::shared_ptr<AithetaFilterCreatorBase>& creator)
{
    if (nullptr == _normalSegment) {
        AUTIL_LOG(WARN, "normal segment is nullptr");
        return {Status::OK(), nullptr};
    }

    std::shared_ptr<MetricReporter> metricReporter = nullptr;
    if (_indexerParam.metricsManager) {
        indexlib::util::MetricProviderPtr provider =
            std::make_shared<indexlib::util::MetricProvider>(_indexerParam.metricsManager->GetMetricsReporter());
        metricReporter = std::make_shared<MetricReporter>(provider, _indexName);
    }
    auto searcher = std::make_shared<NormalSegmentSearcher>(segmentSearchConfig, metricReporter);
    if (!searcher->Init(_normalSegment, segmentBaseDocId, creator)) {
        AUTIL_LOG(ERROR, "init normal segment searcher failed.");
        return {Status::InternalError("init normal segment searcher failed."), nullptr};
    }
    return {Status::OK(), searcher};
}

size_t AithetaDiskIndexer::EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto subDir = indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory)
                      ->GetDirectory(indexConfig->GetIndexName(), false);
    if (!subDir) {
        AUTIL_LOG(ERROR, "fail to get subDir, indexName[%s] indexType[%s]", indexConfig->GetIndexName().c_str(),
                  indexConfig->GetIndexType().c_str());
        return 0ul;
    }
    size_t loadMemory = NormalSegment::EstimateLoadMemoryUse(subDir);

    AUTIL_LOG(INFO, "load memory: %fMB with path[%s]", loadMemory / 1024.0 / 1024.0,
              indexDirectory->DebugString().c_str());
    return loadMemory;
}

size_t AithetaDiskIndexer::EvaluateCurrentMemUsed() { return _currentMemUsed; }
} // namespace indexlibv2::index::ann
