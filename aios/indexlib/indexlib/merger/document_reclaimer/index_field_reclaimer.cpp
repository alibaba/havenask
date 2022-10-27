#include "indexlib/merger/document_reclaimer/index_field_reclaimer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include <beeper/beeper.h>

using namespace std;
using namespace kmonitor;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, IndexFieldReclaimer);

IndexFieldReclaimer::IndexFieldReclaimer(
        const PartitionDataPtr& partitionData,
        const IndexReclaimerParam& reclaimParam,
        misc::MetricProviderPtr metricProvider) 
    : IndexReclaimer(partitionData, reclaimParam, metricProvider)
    , mTerms(reclaimParam.GetReclaimTerms())
{
    IE_INIT_METRIC_GROUP(mMetricProvider, termReclaim,
                         "docReclaim/termReclaim", kmonitor::GAUGE, "count");
}


IndexFieldReclaimer::~IndexFieldReclaimer() 
{
}

bool IndexFieldReclaimer::Init(const IndexConfigPtr& indexConfig,
                               const MultiFieldIndexReaderPtr& indexReaders)
{
    mIndexConfig = indexConfig;
    if (indexReaders)
    {
        mIndexReaders = indexReaders;
    }
    return true;
}

bool IndexFieldReclaimer::Reclaim(const DocumentDeleterPtr& docDeleter)
{
    if (mTerms.empty())
    {
        return true;
    }

    string indexName = mIndexConfig->GetIndexName();
    IndexReaderPtr indexReader = mIndexReaders->GetIndexReader(indexName);
    if (!indexReader)
    {
        indexReader.reset(IndexReaderFactory::CreateIndexReader(
                              mIndexConfig->GetIndexType()));
        if (!indexReader)
        {
            IE_LOG(ERROR, "create index reader for index[%s] failed", 
                   indexName.c_str());
            return false;
        }
        indexReader->Open(mIndexConfig, mPartitionData);
        mIndexReaders->AddIndexReader(mIndexConfig, indexReader);
    }

    for (size_t i = 0; i < mTerms.size(); ++i)
    {
        Term term(mTerms[i], indexName);
        PostingIteratorPtr iterator(indexReader->Lookup(term));
        if (!iterator)
        {
            IE_LOG(INFO, "reclaim_term[%s] does not exist in reclaim_index[%s]", 
                   mTerms[i].c_str(), indexName.c_str());
            continue;
        }
        size_t reclaimDocCount = 0;
        docid_t docId = iterator->SeekDoc(0);
        while (docId != INVALID_DOCID)
        {
            docDeleter->Delete(docId);
            reclaimDocCount++;
            docId = iterator->SeekDoc(docId + 1);
        }
        IE_LOG(INFO, "reclaim_term[%s] matches %zu docs", 
               mTerms[i].c_str(), reclaimDocCount);
        if (reclaimDocCount > 0)
        {
            IE_LOG(INFO, "index [%s] reclaim_term[%s] matches %lu docs",
                   indexName.c_str(), mTerms[i].c_str(), reclaimDocCount);
            MetricsTags tags;
            string reportStr = indexName + "-" + mTerms[i];
            tags.AddTag("term", reportStr);
            IE_REPORT_METRIC_WITH_TAGS(termReclaim, &tags, reclaimDocCount);
        }
    }
    return true;
}

IE_NAMESPACE_END(merger);

