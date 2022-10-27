#ifndef ISEARCH_RANGESCANITERATORWITHOUTFILTER_H
#define ISEARCH_RANGESCANITERATORWITHOUTFILTER_H

#include <ha3/sql/common/common.h>
#include <ha3/search/FilterWrapper.h>
#include <ha3/search/LayerMetas.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>
#include <matchdoc/MatchDocAllocator.h>
#include <ha3/sql/ops/scan/ScanIterator.h>
#include <ha3/common/TimeoutTerminator.h>
BEGIN_HA3_NAMESPACE(sql);

class RangeScanIteratorWithoutFilter : public ScanIterator{
public:
    RangeScanIteratorWithoutFilter(const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                                   const IE_NAMESPACE(index)::DeletionMapReaderPtr &delMapReader,
                                   const search::LayerMetaPtr &layerMeta,
                                   common::TimeoutTerminator *timeoutTerminator = NULL);

    bool batchSeek(size_t batchSize, std::vector<matchdoc::MatchDoc> &matchDocs) override;
    
private:
    IE_NAMESPACE(index)::DeletionMapReaderPtr _delMapReader;
    search::LayerMetaPtr _layerMeta; // hold resource for queryexecutor use raw pointer
    size_t _rangeIdx;
    int32_t _curId;
};

HA3_TYPEDEF_PTR(RangeScanIteratorWithoutFilter);

END_HA3_NAMESPACE(sql);

#endif 
