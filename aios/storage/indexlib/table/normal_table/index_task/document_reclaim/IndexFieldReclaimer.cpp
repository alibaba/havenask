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
#include "indexlib/table/normal_table/index_task/document_reclaim/IndexFieldReclaimer.h"

#include "autil/memory.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/deletionmap/DeletionMapPatchWriter.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"
#include "indexlib/index/inverted_index/PostingIterator.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, IndexFieldReclaimer);

IndexFieldReclaimer::IndexFieldReclaimer(const std::shared_ptr<kmonitor::MetricsReporter>& reporter,
                                         const IndexReclaimerParam& param,
                                         std::map<segmentid_t, docid_t> segmentId2BaseDocId,
                                         std::map<segmentid_t, size_t> segmentId2DocCount)
    : IndexReclaimer(reporter)
    , _param(param)
    , _segmentId2BaseDocId(std::move(segmentId2BaseDocId))
    , _segmentId2DocCount(std::move(segmentId2DocCount))

{
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, termReclaim, "docReclaim/termReclaim", kmonitor::GAUGE);
}

bool IndexFieldReclaimer::Init(const std::shared_ptr<indexlib::index::MultiFieldIndexReader>& multiFieldIndexReader)
{
    assert(_param.GetReclaimOperator().empty());
    _multiFieldIndexReader = multiFieldIndexReader;
    return true;
}

Status IndexFieldReclaimer::Reclaim(index::DeletionMapPatchWriter* docDeleter)
{
    auto terms = _param.GetReclaimTerms();
    if (terms.empty()) {
        return Status::OK();
    }
    const std::string& indexName = _param.GetReclaimIndex();
    for (auto& termStr : terms) {
        int64_t totalReclaimDoc = 0;
        indexlib::index::Term term(termStr, indexName);
        for (auto [segmentId, baseDocId] : _segmentId2BaseDocId) {
            DocIdRange range {baseDocId, baseDocId + _segmentId2DocCount[segmentId]};
            autil::mem_pool::Pool pool;
            auto res = _multiFieldIndexReader->PartialLookup(
                term, {range}, indexlib::index::InvertedIndexReader::DEFAULT_STATE_POOL_SIZE, pt_default, &pool);
            if (!res.Ok()) {
                RETURN_STATUS_ERROR(InternalError, "read inverted index [%s] failed with ec [%s]", indexName.c_str(),
                                    indexlib::index::ErrorCodeToString(res.GetErrorCode()).c_str());
            }
            auto iter = autil::shared_ptr_pool(&pool, res.Value());
            if (!iter) {
                AUTIL_LOG(INFO, "reclaim_term[%s] does not exist in reclaim_index[%s] for segment [%d]",
                          termStr.c_str(), indexName.c_str(), segmentId);
                continue;
            }
            size_t reclaimDocCount = 0;
            docid_t docId = iter->SeekDoc(0);
            while (docId != INVALID_DOCID && docId != indexlib::END_DOCID) {
                assert(docId >= baseDocId);
                auto status = docDeleter->Write(segmentId, docId - baseDocId);
                assert(status.IsOK());
                RETURN_IF_STATUS_ERROR(status, "");
                reclaimDocCount++;
                docId = iter->SeekDoc(docId + 1);
            }
            totalReclaimDoc += reclaimDocCount;
            AUTIL_LOG(INFO, "reclaim_term[%s] matches %zu docs for segment [%d]", termStr.c_str(), reclaimDocCount,
                      segmentId);
        }
        kmonitor::MetricsTags tags;
        std::string reclaimTerm = indexName + "-" + termStr;
        tags.AddTag("term", reclaimTerm);
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&tags, termReclaim, totalReclaimDoc);
        AUTIL_LOG(INFO, "total reclaim  doc [%ld]", totalReclaimDoc);
    }
    return Status::OK();
}

} // namespace indexlibv2::table
