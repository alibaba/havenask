#ifndef ISEARCH_HA3SCANITERATOR_H
#define ISEARCH_HA3SCANITERATOR_H

#include <ha3/sql/common/common.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/search/SingleLayerSearcher.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <matchdoc/MatchDocAllocator.h>
#include <indexlib/partition/index_partition_reader.h>
#include <indexlib/index/normal/attribute/accessor/join_docid_attribute_iterator.h>
#include <ha3/sql/ops/scan/ScanIterator.h>
BEGIN_HA3_NAMESPACE(sql);

struct Ha3ScanIteratorParam {
    Ha3ScanIteratorParam()
        : timeoutTerminator(NULL)
        , matchDataManager(NULL)
        , needAllSubDocFlag(false)
    {
    }
    search::QueryExecutorPtr queryExecutor;
    search::FilterWrapperPtr filterWrapper;
    matchdoc::MatchDocAllocatorPtr matchDocAllocator;
    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    IE_NAMESPACE(index)::DeletionMapReaderPtr subDelMapReader;
    search::LayerMetaPtr layerMeta; 
    IE_NAMESPACE(index)::JoinDocidAttributeIterator *mainToSubIt;
    common::TimeoutTerminator *timeoutTerminator;
    search::MatchDataManager *matchDataManager;
    bool needAllSubDocFlag;
};


class Ha3ScanIterator : public ScanIterator {
public:
    Ha3ScanIterator(const Ha3ScanIteratorParam &param);
    virtual ~Ha3ScanIterator();
public:
    bool batchSeek(size_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) override;
    uint32_t getTotalScanCount() override;

private:
    search::FilterWrapperPtr _filterWrapper;
    search::QueryExecutorPtr _queryExecutor;
    IE_NAMESPACE(index)::DeletionMapReaderPtr _delMapReader;
    IE_NAMESPACE(index)::DeletionMapReaderPtr _subDelMapReader;
    search::LayerMetaPtr _layerMeta; // hold resource for queryexecutor use raw pointer
    search::SingleLayerSearcherPtr _singleLayerSearcher;
    bool _needSubDoc;
    bool _eof;
};

HA3_TYPEDEF_PTR(Ha3ScanIterator);

END_HA3_NAMESPACE(sql);

#endif 
