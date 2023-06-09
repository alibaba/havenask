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
#include "indexlib/merger/document_reclaimer/and_index_reclaimer.h"

#include "beeper/beeper.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/index/inverted_index/AndPostingExecutor.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/TermPostingExecutor.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/merger/document_reclaimer/document_deleter.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"

using namespace std;
using namespace beeper;
using namespace kmonitor;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using indexlib::index::AndPostingExecutor;
using indexlib::index::PostingExecutor;
using indexlib::index::TermPostingExecutor;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, AndIndexReclaimer);

AndIndexReclaimer::AndIndexReclaimer(const index_base::PartitionDataPtr& partitionData,
                                     const IndexReclaimerParam& reclaimParam, util::MetricProviderPtr metricProvider)
    : IndexReclaimer(partitionData, reclaimParam, metricProvider)
{
    IE_INIT_METRIC_GROUP(mMetricProvider, AndReclaim, "docReclaim/andReclaim", kmonitor::GAUGE, "count");
}

AndIndexReclaimer::~AndIndexReclaimer() {}

bool AndIndexReclaimer::Init(const IndexSchemaPtr& indexSchema,
                             const index::legacy::MultiFieldIndexReaderPtr& indexReaders)
{
    assert(indexSchema);
    if (indexReaders) {
        mIndexReaders = indexReaders;
    }

    if (mReclaimParam.GetReclaimOperator() != IndexReclaimerParam::AND_RECLAIM_OPERATOR) {
        IE_LOG(ERROR, "invalid reclaimOperator [%s]", mReclaimParam.GetReclaimOperator().c_str());
        return false;
    }

    int32_t count = 0;
    for (const auto& oprand : mReclaimParam.GetReclaimOprands()) {
        if (mIndexReaders->GetInvertedIndexReader(oprand.indexName) != NULL) {
            ++count;
            continue;
        }
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(oprand.indexName);
        if (!indexConfig) {
            IE_LOG(ERROR, "indexConfig[%s] is not existed!", oprand.indexName.c_str());
            return false;
        }
        std::shared_ptr<InvertedIndexReader> indexReader(
            IndexReaderFactory::CreateIndexReader(indexConfig->GetInvertedIndexType(), /*indexMetrics*/ nullptr));
        if (!indexReader) {
            IE_LOG(ERROR, "create index reader for index[%s] failed", oprand.indexName.c_str());
            return false;
        }
        const auto& legacyReader = std::dynamic_pointer_cast<index::LegacyIndexReaderInterface>(indexReader);
        assert(legacyReader);
        legacyReader->Open(indexConfig, mPartitionData);
        mIndexReaders->AddIndexReader(indexConfig, indexReader);
        ++count;
    }

    if (count == 0) {
        string paramStr = ToJsonString(mReclaimParam);
        IE_LOG(WARN, "No valid index found in reclaim parameters[%s]", paramStr.c_str());
    }
    return true;
}

bool AndIndexReclaimer::Reclaim(const DocumentDeleterPtr& docDeleter)
{
    string conditionStr;
    string monitorReport;
    std::vector<shared_ptr<PostingExecutor>> postingExecutors;
    postingExecutors.reserve(mReclaimParam.GetReclaimOprands().size());
    for (const auto& oprand : mReclaimParam.GetReclaimOprands()) {
        const std::shared_ptr<InvertedIndexReader>& indexReader =
            mIndexReaders->GetInvertedIndexReader(oprand.indexName);
        if (!indexReader) {
            IE_LOG(ERROR, "find index reader for index[%s] failed, not reclaim", oprand.indexName.c_str());
            return true;
        }

        Term term(oprand.term, oprand.indexName);
        std::shared_ptr<PostingIterator> iterator(indexReader->Lookup(term).ValueOrThrow());
        if (!iterator) {
            IE_LOG(INFO, "reclaim_term[%s] does not exist in reclaim_index[%s], not reclaim", oprand.term.c_str(),
                   oprand.indexName.c_str());
            return true;
        }
        shared_ptr<PostingExecutor> executor(new TermPostingExecutor(iterator));
        postingExecutors.push_back(executor);
        conditionStr += (oprand.indexName + "=" + oprand.term + ";");
        // tag not support "="
        monitorReport += (oprand.indexName + "-" + oprand.term + "-");
    }

    if (postingExecutors.size() == 0) {
        IE_LOG(INFO, "reclaim_operator[AND] matches 0 docs");
        return true;
    }

    shared_ptr<AndPostingExecutor> andOp(new AndPostingExecutor(postingExecutors));
    size_t reclaimDocCount = 0;
    docid_t docId = andOp->Seek(0);
    while (docId != INVALID_DOCID && docId != END_DOCID) {
        docDeleter->Delete(docId);
        reclaimDocCount++;
        docId = andOp->Seek(docId + 1);
    }

    IE_LOG(INFO, "reclaim_operator[AND] matches %zu docs", reclaimDocCount);
    if (reclaimDocCount > 0) {
        IE_LOG(INFO, "[%s] reclaim_operator[AND] matches %lu docs", conditionStr.c_str(), reclaimDocCount);
        MetricsTags tags;
        tags.AddTag("condition", monitorReport.c_str());
        IE_REPORT_METRIC_WITH_TAGS(AndReclaim, &tags, reclaimDocCount);
    }
    return true;
}
}} // namespace indexlib::merger
