#ifndef ISEARCH_RANGESCANITERATOR_H
#define ISEARCH_RANGESCANITERATOR_H

#include <ha3/sql/common/common.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/sql/ops/scan/ScanIterator.h>
#include <ha3/common/TimeoutTerminator.h>
BEGIN_HA3_NAMESPACE(sql);

class RangeScanIterator : public ScanIterator {
public:
    RangeScanIterator(const search::FilterWrapperPtr &filterWrapper,
                      const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                      const IE_NAMESPACE(index)::DeletionMapReaderPtr &delMapReader,
                      const search::LayerMetaPtr &layerMeta,
                      common::TimeoutTerminator *timeoutTerminator = NULL);

    bool batchSeek(size_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) override;
    
private:
    size_t batchFilter(const std::vector<int32_t> &docIds, std::vector<matchdoc::MatchDoc> &matchDocs);
    
private: 
    search::FilterWrapperPtr _filterWrapper;
    IE_NAMESPACE(index)::DeletionMapReaderPtr _delMapReader;
    search::LayerMetaPtr _layerMeta; // hold resource for queryexecutor use raw pointer
    size_t _rangeIdx;
    int32_t _curId;
};

HA3_TYPEDEF_PTR(RangeScanIterator);

END_HA3_NAMESPACE(sql);

#endif 
