#ifndef ISEARCH_QUERYSCANITERATOR_H
#define ISEARCH_QUERYSCANITERATOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/sql/ops/scan/ScanIterator.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/search/LayerMetas.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/common/TimeoutTerminator.h>
BEGIN_HA3_NAMESPACE(sql);

class QueryScanIterator : public ScanIterator {
public:
    QueryScanIterator(const search::QueryExecutorPtr &queryExecutor,
                     const search::FilterWrapperPtr &filterWrapper,
                     const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                     const IE_NAMESPACE(index)::DeletionMapReaderPtr &delMapReader,
                      const search::LayerMetaPtr &layerMeta,
                      common::TimeoutTerminator *_timeoutTerminator = NULL);
    virtual ~QueryScanIterator();
private:
    QueryScanIterator(const QueryScanIterator &);
    QueryScanIterator& operator=(const QueryScanIterator &);
public:
    bool batchSeek(size_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) override;
private:
    inline bool tryToMakeItInRange(docid_t &docId);
    bool moveToCorrectRange(docid_t &docId);

    size_t batchFilter(const std::vector<int32_t> &docIds, std::vector<matchdoc::MatchDoc> &matchDocs);

private:
    search::QueryExecutorPtr _queryExecutor;
    search::FilterWrapperPtr _filterWrapper;
    IE_NAMESPACE(index)::DeletionMapReaderPtr _deletionMapReader;
    search::LayerMetaPtr _layerMeta; 
    IE_NAMESPACE(index)::PostingIterator *_postingIter;
    docid_t _curDocId;
    docid_t _curBegin;
    docid_t _curEnd;
    size_t _rangeCousor;    
    int64_t _batchFilterTime;
    int64_t _scanTime;
private:
    HA3_LOG_DECLARE();
};


inline bool QueryScanIterator::tryToMakeItInRange(docid_t &docId) {
    if (_curEnd >= docId) {
        return true;
    }
    return moveToCorrectRange(docId);
}

HA3_TYPEDEF_PTR(QueryScanIterator);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_TERMSCANITERATOR_H
