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
#ifndef __INDEXLIB_AITHETA_INDEX_RETRIEVER_H
#define __INDEXLIB_AITHETA_INDEX_RETRIEVER_H

#include <aitheta/index_framework.h>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/log.h"
#include "indexlib_plugin/plugins/aitheta/aitheta_index_segment_retriever.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/index_segment_reader.h"
#include "indexlib_plugin/plugins/aitheta/util/metric_reporter.h"
#include "indexlib_plugin/plugins/aitheta/util/recall_reporter.h"

namespace indexlib {
namespace aitheta_plugin {

class AithetaIndexRetriever : public indexlib::index::IndexRetriever {
public:
    AithetaIndexRetriever(const util::KeyValueMap &parameters) : mSchemaParam(parameters), mRtSgtNo(INVALID_SGT_NO) {}
    ~AithetaIndexRetriever() = default;

public:
    bool Init(const indexlib::index::DeletionMapReaderPtr &deletionMapReader,
              const std::vector<indexlib::index::IndexSegmentRetrieverPtr> &retrievers,
              const IndexerResourcePtr &resource = IndexerResourcePtr()) override;
    indexlib::index::Result<std::vector<SegmentMatchInfo>> Search(const indexlib::index::Term &term, autil::mem_pool::Pool *pool) override;

private:
    bool InitRtSgtReader(const ContextHolderPtr &holder,
                         docid_t rtSgtBaseDocId,
                         const indexlib::index::DeletionMapReaderPtr &delMapRr);
    void InitQueryTopkRatio();
    bool InitRecallReporter();
    bool ParseQuery(const std::string &query, std::vector<Query> &queries);

private:
    SchemaParameter mSchemaParam;
    std::vector<IndexSegmentReaderPtr> mSgtReaders;
    MetricReporterPtr _metricReporter;
    RecallReporterPtr mRecallReporter;
    int32_t mRtSgtNo;

private:
    IE_LOG_DECLARE();

private:
    METRIC_DECLARE(mTotalSeekCount);
    METRIC_DECLARE(mTotalSeekLatency);
    METRIC_DECLARE(mRtSgtSeekLatency);
    METRIC_DECLARE(mFullSgtSeekLatency);
};

DEFINE_SHARED_PTR(AithetaIndexRetriever);

} // namespace aitheta_plugin
} // namespace indexlib

#endif //__INDEXLIB_AITHETA_INDEX_RETRIEVER_H
