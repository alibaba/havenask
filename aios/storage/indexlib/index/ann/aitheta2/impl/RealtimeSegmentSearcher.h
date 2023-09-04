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
#include "indexlib/index/ann/aitheta2/impl/RealtimeIndexSearcher.h"
#include "indexlib/index/ann/aitheta2/impl/RealtimeSegment.h"
#include "indexlib/index/ann/aitheta2/impl/SegmentSearcher.h"

namespace indexlibv2::index::ann {

class RealtimeSegmentSearcher : public SegmentSearcher
{
public:
    RealtimeSegmentSearcher(const AithetaIndexConfig& config, const MetricReporterPtr& metricReporter)
        : SegmentSearcher(config, metricReporter)
    {
    }
    ~RealtimeSegmentSearcher() = default;

public:
    bool Init(const SegmentPtr& segment, docid_t segmentBaseDocId,
              const std::shared_ptr<AithetaFilterCreatorBase>& creator) override;

protected:
    bool DoSearch(const AithetaQueries& indexQuery, const std::shared_ptr<AithetaAuxSearchInfoBase>& searchInfo,
                  ResultHolder& holder) override;

    RealtimeIndexSearcherPtr GetRealtimeIndexSearcher(index_id_t indexId);

protected:
    RealtimeSegmentPtr _segment;
    mutable autil::ReadWriteLock _readWriteLock;
    RealtimeIndexSearcherMap _indexSearcherMap;
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<RealtimeSegmentSearcher> RealtimeSegmentSearcherPtr;

} // namespace indexlibv2::index::ann
