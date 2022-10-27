#ifndef ISEARCH_SUMMARYSEARCHER_H
#define ISEARCH_SUMMARYSEARCHER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/summary/SummaryExtractorChain.h>
#include <ha3/summary/SummaryProfileManager.h>
#include <ha3/search/SummaryFetcher.h>
#include <ha3/search/IndexPartitionWrapper.h>
#include <ha3/search/SearchCommonResource.h>

BEGIN_HA3_NAMESPACE(search);

class SummarySearcher
{
public:
    SummarySearcher(SearchCommonResource& resource,
                    IndexPartitionWrapperPtr indexPartittionWrapper,
                    const summary::SummaryProfileManagerPtr& summaryProfileManager,
                    const config::HitSummarySchemaPtr &hitSummarySchema);
    ~SummarySearcher();
private:
    SummarySearcher(const SummarySearcher &);
    SummarySearcher& operator=(const SummarySearcher &);
public:
    common::ResultPtr search(const common::Request *request,
                             const proto::Range &hashIdRange,
                             FullIndexVersion fullIndexVersion);

    common::ResultPtr extraSearch(const common::Request *request,
                                  common::ResultPtr inputResult,
                                  const std::string& tableName);
    
    void setIndexPartitionReaderWrapper(
            const IndexPartitionReaderWrapperPtr indexPartitionReaderWrapper)
    {
        _indexPartitionReaderWrapper = indexPartitionReaderWrapper;
    }
    
    void setPartitionReaderSnapshot(
            const IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &snapshot)
    {
        _partitionReaderSnapshot = snapshot;
    }
private:
    bool doSearch(const common::GlobalIdVector &gids,
                  const common::Request *request,
                  common::Result* result);
    void doSearch(const common::GlobalIdVector &gids,
                  bool allowLackOfSummary,
                  int64_t signature,
                  autil::mem_pool::Pool *pool,
                  common::Result* result,
                  summary::SummaryExtractorChain *summaryExtractorChain,
                  summary::SummaryExtractorProvider *summaryExractorProvider);

private:
    void fetchSummaryByDocId(
            common::Result *result,
            const common::Request *request,
            const proto::Range &hashIdRange,
            FullIndexVersion fullIndexVersion);
    void fetchSummaryByPk(
            common::Result *result,
            const common::Request *request,
            const proto::Range &hashIdRange);
    bool createIndexPartReaderWrapper(FullIndexVersion fullIndexVersion);
    bool createIndexPartReaderWrapperForExtra(const std::string& tableName);
    const summary::SummaryProfile *getSummaryProfile(
            const common::Request *request,
            summary::SummaryProfileManagerPtr summaryProfileManager) ;
    bool convertPK2DocId(common::GlobalIdVector &gids,
                         const IndexPartitionReaderWrapperPtr& indexPartReaderWrapper,
                         bool ignoreDelete);
    template <typename PKType>
    bool convertPK2DocId(common::GlobalIdVector &gids,
                         const IE_NAMESPACE(index)::IndexReaderPtr& pkIndexReader,
                         bool ignoreDelete);
    void fillHits(common::Hits *hits,
                  summary::SummaryExtractorChain *summaryExtractorChain,
                  summary::SummaryExtractorProvider *summaryExractorProvider,
                  bool allowLackOfSummary);
    void fetchSummaryDocuments(common::Hits *hits,
                               summary::SummaryExtractorProvider *summaryExractorProvider,
                               bool allowLackOfSummary);
    inline SummaryFetcherPtr createSummaryFetcher();
    void doFetchSummaryDocument(SummaryFetcher &summaryFetcher,
                                common::Hits *hits,
                                bool allowLackOfSummary);
    inline void resetGids(common::Result *result,
                          const common::GlobalIdVector &originalGids);
    bool declareAttributeFieldsToSummary(summary::SummaryExtractorProvider *summaryExtractorProvider);
    void genSummaryGroupIdVec(common::ConfigClause *configClause);
    // for test
    const SummaryGroupIdVec &getSummaryGroupIdVec() const { return _summaryGroupIdVec; }
    common::ResultPtr doExtraSearch(const common::Request *request,
                                    const common::ResultPtr &inputResult);

private:
    search::SearchCommonResource& _resource;
    IndexPartitionWrapperPtr _indexPartittionWrapper;
    IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _partitionReaderSnapshot;
    const summary::SummaryProfileManagerPtr& _summaryProfileManager;
    config::HitSummarySchemaPtr _hitSummarySchema;
    common::Tracer::TraceMode _traceMode;
    int32_t _traceLevel;
    SummaryGroupIdVec _summaryGroupIdVec;
    SummarySearchType _summarySearchType;
private:
    friend class SummarySearcherTest;
    // for test
    suez::turing::AttributeExpressionCreator *_attributeExpressionCreator;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SummarySearcher);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_SUMMARYSEARCHER_H
