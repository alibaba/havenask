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

#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/AithetaFilterCreator.h"
#include "indexlib/index/ann/aitheta2/AithetaAuxSearchInfoBase.h"
#include "indexlib/index/ann/aitheta2/impl/Segment.h"
#include "indexlib/index/ann/aitheta2/util/MetricReporter.h"
#include "indexlib/index/ann/aitheta2/util/ResultHolder.h"
#include "aios/storage/indexlib/index/ann/aitheta2/proto/AithetaQuery.pb.h"

namespace indexlibv2::index::ann {

class SegmentSearcher
{
public:
    SegmentSearcher(const AithetaIndexConfig& config, const MetricReporterPtr& metricReporter)
        : _indexConfig(config)
        , _metricReporter(metricReporter)
    {
    }

    virtual ~SegmentSearcher() { AUTIL_LOG(INFO, "segment searcher[%d] released", _segmentBaseDocId); }

public:
    virtual bool Init(const SegmentPtr& segment, docid_t segmentBaseDocId,
                      const std::shared_ptr<AithetaFilterCreatorBase>& creator) = 0;
    bool Search(const AithetaQueries& query, const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo, ResultHolder& holder);

protected:
    void InitSearchMetrics();
    virtual bool DoSearch(const AithetaQueries& query, const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                          ResultHolder& holder) = 0;

protected:
    AithetaIndexConfig _indexConfig;
    docid_t _segmentBaseDocId;
    std::shared_ptr<AithetaFilterCreatorBase> _creator;
    MetricReporterPtr _metricReporter;

protected:
    METRIC_DECLARE(_resultCountMetric);
    METRIC_DECLARE(_seekLatencyMetric);
    METRIC_DECLARE(_filteredCountMetric);
    METRIC_DECLARE(_distCalcCountMetric);

    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<SegmentSearcher> SegmentSearcherPtr;

} // namespace indexlibv2::index::ann
