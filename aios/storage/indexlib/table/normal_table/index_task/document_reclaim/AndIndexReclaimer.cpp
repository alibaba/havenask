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
#include "indexlib/table/normal_table/index_task/document_reclaim/AndIndexReclaimer.h"

#include "autil/memory.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/deletionmap/DeletionMapPatchWriter.h"
#include "indexlib/index/inverted_index/AndPostingExecutor.h"
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"
#include "indexlib/index/inverted_index/PostingExecutor.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/TermPostingExecutor.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, AndIndexReclaimer);

AndIndexReclaimer::AndIndexReclaimer(const std::shared_ptr<kmonitor::MetricsReporter>& reporter,
                                     const IndexReclaimerParam& param,
                                     std::map<segmentid_t, docid_t> segmentId2BaseDocId,
                                     std::map<segmentid_t, size_t> segmentId2DocCount)
    : IndexReclaimer(reporter)
    , _param(param)
    , _segmentId2BaseDocId(std::move(segmentId2BaseDocId))
    , _segmentId2DocCount(std::move(segmentId2DocCount))
{
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, andReclaim, "docReclaim/andReclaim", kmonitor::GAUGE);
}

bool AndIndexReclaimer::Init(const std::shared_ptr<indexlib::index::MultiFieldIndexReader>& multiFieldIndexReader)
{
    assert(_param.GetReclaimOperator() == IndexReclaimerParam::AND_RECLAIM_OPERATOR);
    _multiFieldIndexReader = multiFieldIndexReader;
    return true;
}

Status AndIndexReclaimer::Reclaim(index::DeletionMapPatchWriter* docDeleter)
{
    std::string reportTags;
    if (_param.GetReclaimOprands().empty()) {
        AUTIL_LOG(WARN, "No valid index (oprand) found in reclaim params [%s]",
                  autil::legacy::ToJsonString(_param, true).c_str());
        return Status::OK();
    }
    for (const auto& oprand : _param.GetReclaimOprands()) {
        reportTags += (oprand.indexName + "-" + oprand.term + "-");
    }

    int64_t totalReclaimDoc = 0;
    autil::mem_pool::Pool pool;

    for (auto [segmentId, baseDocId] : _segmentId2BaseDocId) {
        pool.reset();
        std::string conditionStr;
        std::string monitorReport;
        std::vector<std::shared_ptr<indexlib::index::PostingExecutor>> postingExecutors;
        postingExecutors.reserve(_param.GetReclaimOprands().size());
        DocIdRange range {baseDocId, baseDocId + _segmentId2DocCount[segmentId]};
        for (const auto& oprand : _param.GetReclaimOprands()) {
            const std::string& indexName = oprand.indexName;
            conditionStr += (indexName + "=" + oprand.term + ";");
            indexlib::index::Term term(oprand.term, indexName);
            auto res = _multiFieldIndexReader->PartialLookup(
                term, {range}, indexlib::index::InvertedIndexReader::DEFAULT_STATE_POOL_SIZE, pt_default, &pool);
            if (!res.Ok()) {
                RETURN_STATUS_ERROR(InternalError, "read inverted index [%s] failed with ec [%s]", indexName.c_str(),
                                    indexlib::index::ErrorCodeToString(res.GetErrorCode()).c_str());
            }
            auto iter = autil::shared_ptr_pool(&pool, res.Value());
            if (!iter) {
                AUTIL_LOG(INFO, "reclaim_term[%s] does not exist in reclaim_index[%s] for segment [%d], not reclaim",
                          oprand.term.c_str(), indexName.c_str(), segmentId);
                break;
            }
            auto executor = std::make_shared<indexlib::index::TermPostingExecutor>(iter);
            postingExecutors.push_back(executor);
        }
        size_t reclaimDocCount = 0;
        if (postingExecutors.size() == _param.GetReclaimOprands().size()) {
            auto andOp = std::make_shared<indexlib::index::AndPostingExecutor>(postingExecutors);
            docid_t docId = andOp->Seek(0);
            while (docId != INVALID_DOCID && docId != indexlib::END_DOCID) {
                assert(docId >= baseDocId);
                auto status = docDeleter->Write(segmentId, docId - baseDocId);
                assert(status.IsOK());
                RETURN_IF_STATUS_ERROR(status, "");
                reclaimDocCount++;
                docId = andOp->Seek(docId + 1);
            }
        }
        totalReclaimDoc += reclaimDocCount;
        AUTIL_LOG(INFO, "reclaim_operator[AND] matches %zu docs for segment [%d] by condition [%s]", reclaimDocCount,
                  segmentId, conditionStr.c_str());
    }
    kmonitor::MetricsTags tags;
    tags.AddTag("condition", reportTags.c_str());
    INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&tags, andReclaim, totalReclaimDoc);
    return Status::OK();
}

} // namespace indexlibv2::table
