#ifndef __INDEXLIB_DOC_RANGE_PARTITIONER_H
#define __INDEXLIB_DOC_RANGE_PARTITIONER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/partition_info.h"

IE_NAMESPACE_BEGIN(index);

class DocRangePartitioner
{
public:
    static bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint,
        const index::PartitionInfoPtr& partitionInfo, size_t totalWayCount, size_t wayIdx,
        DocIdRangeVector& ranges);
    static bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint,
        const index::PartitionInfoPtr& partitionInfo, size_t totalWayCount,
        std::vector<DocIdRangeVector>& rangeVectors);
    static bool Validate(const DocIdRangeVector& ranges);

private:
    static bool InRange(const DocIdRange& inner, const DocIdRange& outter)
    {
        return inner.first >= outter.first && inner.second <= outter.second;
    }

    static DocIdRangeVector::const_iterator IntersectRange(
        const DocIdRangeVector::const_iterator& begin1,
        const DocIdRangeVector::const_iterator& end1,
        const DocIdRangeVector::const_iterator& begin2,
        const DocIdRangeVector::const_iterator& end2, DocIdRangeVector& output);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocRangePartitioner);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_RANGE_PARTITIONER_H
