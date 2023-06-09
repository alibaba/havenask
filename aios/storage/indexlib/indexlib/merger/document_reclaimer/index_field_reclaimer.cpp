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
#include "indexlib/merger/document_reclaimer/index_field_reclaimer.h"

#include "beeper/beeper.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"

using namespace std;
using namespace kmonitor;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::common;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, IndexFieldReclaimer);

IndexFieldReclaimer::IndexFieldReclaimer(const PartitionDataPtr& partitionData, const IndexReclaimerParam& reclaimParam,
                                         util::MetricProviderPtr metricProvider)
    : IndexReclaimer(partitionData, reclaimParam, metricProvider)
    , mTerms(reclaimParam.GetReclaimTerms())
{
    IE_INIT_METRIC_GROUP(mMetricProvider, termReclaim, "docReclaim/termReclaim", kmonitor::GAUGE, "count");
}

IndexFieldReclaimer::~IndexFieldReclaimer() {}

bool IndexFieldReclaimer::Init(const IndexConfigPtr& indexConfig,
                               const index::legacy::MultiFieldIndexReaderPtr& indexReaders)
{
    mIndexConfig = indexConfig;
    if (indexReaders) {
        mIndexReaders = indexReaders;
    }
    return true;
}

bool IndexFieldReclaimer::Reclaim(const DocumentDeleterPtr& docDeleter)
{
    if (mTerms.empty()) {
        return true;
    }

    string indexName = mIndexConfig->GetIndexName();
    std::shared_ptr<InvertedIndexReader> indexReader = mIndexReaders->GetInvertedIndexReader(indexName);
    if (!indexReader) {
        std::shared_ptr<InvertedIndexReader> reader(
            IndexReaderFactory::CreateIndexReader(mIndexConfig->GetInvertedIndexType(), /*indexMetrics*/ nullptr));
        if (!reader) {
            IE_LOG(ERROR, "create index reader for index[%s] failed", indexName.c_str());
            return false;
        }
        const auto& legacyReader = std::dynamic_pointer_cast<index::LegacyIndexReaderInterface>(reader);
        assert(legacyReader);
        legacyReader->Open(mIndexConfig, mPartitionData);
        indexReader = reader;
        mIndexReaders->AddIndexReader(mIndexConfig, reader);
    }

    for (size_t i = 0; i < mTerms.size(); ++i) {
        Term term(mTerms[i], indexName);
        std::shared_ptr<PostingIterator> iterator(indexReader->Lookup(term).ValueOrThrow());
        if (!iterator) {
            IE_LOG(INFO, "reclaim_term[%s] does not exist in reclaim_index[%s]", mTerms[i].c_str(), indexName.c_str());
            continue;
        }
        size_t reclaimDocCount = 0;
        docid_t docId = iterator->SeekDoc(0);
        while (docId != INVALID_DOCID) {
            docDeleter->Delete(docId);
            reclaimDocCount++;
            docId = iterator->SeekDoc(docId + 1);
        }
        IE_LOG(INFO, "reclaim_term[%s] matches %zu docs", mTerms[i].c_str(), reclaimDocCount);
        if (reclaimDocCount > 0) {
            IE_LOG(INFO, "index [%s] reclaim_term[%s] matches %lu docs", indexName.c_str(), mTerms[i].c_str(),
                   reclaimDocCount);
            MetricsTags tags;
            string reportStr = indexName + "-" + mTerms[i];
            tags.AddTag("term", reportStr);
            IE_REPORT_METRIC_WITH_TAGS(termReclaim, &tags, reclaimDocCount);
        }
    }
    return true;
}
}} // namespace indexlib::merger
