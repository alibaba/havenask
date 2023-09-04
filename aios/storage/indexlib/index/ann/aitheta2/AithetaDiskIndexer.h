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

#include "autil/Log.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegment.h"
#include "indexlib/index/ann/aitheta2/impl/NormalSegmentSearcher.h"

namespace indexlibv2::index::ann {

class AithetaDiskIndexer : public IDiskIndexer
{
public:
    AithetaDiskIndexer(const IndexerParameter& indexerParam);
    ~AithetaDiskIndexer() = default;

    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;
    std::pair<Status, std::shared_ptr<NormalSegmentSearcher>>
    CreateSearcher(docid_t segmentBaseDocId, const std::shared_ptr<AithetaFilterCreatorBase>& creator);

private:
    static constexpr double CONVERT_TO_MB_FACTOR = 1.1f;

private:
    AithetaIndexConfig _aithetaIndexConfig;
    IndexerParameter _indexerParam;
    std::shared_ptr<NormalSegment> _normalSegment;
    std::string _indexName;
    size_t _currentMemUsed = 0;
    MetricReporterPtr _metricReporter;

    AUTIL_LOG_DECLARE();
};
} // namespace indexlibv2::index::ann
