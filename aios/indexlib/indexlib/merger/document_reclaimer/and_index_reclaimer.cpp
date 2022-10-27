#include "indexlib/merger/document_reclaimer/and_index_reclaimer.h"
#include "indexlib/index/normal/inverted_index/accessor/index_reader_factory.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/merger/document_reclaimer/and_posting_executor.h"
#include "indexlib/merger/document_reclaimer/term_posting_executor.h"
#include "indexlib/merger/document_reclaimer/document_deleter.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"
#include <beeper/beeper.h>

using namespace std;
using namespace beeper;
using namespace kmonitor;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, AndIndexReclaimer);

AndIndexReclaimer::AndIndexReclaimer(
    const index_base::PartitionDataPtr& partitionData,
    const IndexReclaimerParam& reclaimParam,
    misc::MetricProviderPtr metricProvider)
    : IndexReclaimer(partitionData, reclaimParam, metricProvider)
{
    IE_INIT_METRIC_GROUP(mMetricProvider, AndReclaim,
                         "docReclaim/andReclaim", kmonitor::GAUGE, "count");
}

AndIndexReclaimer::~AndIndexReclaimer() 
{
}

bool AndIndexReclaimer::Init(const IndexSchemaPtr& indexSchema,
                             const MultiFieldIndexReaderPtr& indexReaders)
{
    assert(indexSchema);
    if (indexReaders)
    {
        mIndexReaders = indexReaders;
    }
    
    if (mReclaimParam.GetReclaimOperator() != IndexReclaimerParam::AND_RECLAIM_OPERATOR)
    {
        IE_LOG(ERROR, "invalid reclaimOperator [%s]",
               mReclaimParam.GetReclaimOperator().c_str());
        return false;
    }

    int32_t count = 0;
    for (const auto& oprand: mReclaimParam.GetReclaimOprands())
    {
        if (mIndexReaders->GetIndexReader(oprand.indexName) != NULL)
        {
            ++count;
            continue;
        }
        IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(oprand.indexName);
        if (!indexConfig)
        {
            IE_LOG(ERROR, "indexConfig[%s] is not existed!",oprand.indexName.c_str());
            return false;            
        }
        IndexReaderPtr indexReader(
            IndexReaderFactory::CreateIndexReader(indexConfig->GetIndexType()));
        if (!indexReader)
        {
            IE_LOG(ERROR, "create index reader for index[%s] failed", 
                   oprand.indexName.c_str());
            return false;
        }
        indexReader->Open(indexConfig, mPartitionData);
        mIndexReaders->AddIndexReader(indexConfig, indexReader);
        ++count;
    }
    
    if (count == 0)
    {
        string paramStr = ToJsonString(mReclaimParam);
        IE_LOG(WARN, "No valid index found in reclaim parameters[%s]",
               paramStr.c_str()); 
    }
    return true;
}

bool AndIndexReclaimer::Reclaim(const DocumentDeleterPtr& docDeleter)
{
    string conditionStr;
    string monitorReport;
    std::vector<PostingExecutorPtr> postingExecutors;
    postingExecutors.reserve(mReclaimParam.GetReclaimOprands().size());
    for (const auto& oprand: mReclaimParam.GetReclaimOprands())
    {
        const IndexReaderPtr& indexReader = mIndexReaders->GetIndexReader(oprand.indexName);
        if (!indexReader)
        {
            IE_LOG(ERROR, "find index reader for index[%s] failed, not reclaim", 
                   oprand.indexName.c_str());
            return true;
        }

        Term term(oprand.term, oprand.indexName);
        PostingIteratorPtr iterator(indexReader->Lookup(term));
        if (!iterator)
        {
            IE_LOG(INFO, "reclaim_term[%s] does not exist in reclaim_index[%s], not reclaim", 
                   oprand.term.c_str(), oprand.indexName.c_str());
            return true;
        }
        PostingExecutorPtr executor(new TermPostingExecutor(iterator));
        postingExecutors.push_back(executor);
        conditionStr += (oprand.indexName + "=" + oprand.term + ";");
        // tag not support "="
        monitorReport += (oprand.indexName + "-" + oprand.term + "-");
    }

    if (postingExecutors.size() == 0) {
        IE_LOG(INFO, "reclaim_operator[AND] matches 0 docs");
        return true;
    }

    AndPostingExecutorPtr andOp(new AndPostingExecutor(postingExecutors));
    size_t reclaimDocCount = 0;
    docid_t docId = andOp->Seek(0);
    while (docId != INVALID_DOCID && docId != END_DOCID)
    {
        docDeleter->Delete(docId); 
        reclaimDocCount++; 
        docId = andOp->Seek(docId + 1); 
    }
    
    IE_LOG(INFO, "reclaim_operator[AND] matches %zu docs", reclaimDocCount);
    if (reclaimDocCount > 0)
    {
        IE_LOG(INFO, "[%s] reclaim_operator[AND] matches %lu docs",
               conditionStr.c_str(), reclaimDocCount);
        MetricsTags tags;
        tags.AddTag("condition", monitorReport.c_str());
        IE_REPORT_METRIC_WITH_TAGS(AndReclaim, &tags, reclaimDocCount);
    }
    return true;
}

IE_NAMESPACE_END(merger);

