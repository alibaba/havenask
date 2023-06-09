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
#ifndef __INDEXLIB_DOC_RANGE_PARTITIONER_H
#define __INDEXLIB_DOC_RANGE_PARTITIONER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

class DocRangePartitioner
{
public:
    static bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, const index::PartitionInfoPtr& partitionInfo,
                                     size_t totalWayCount, size_t wayIdx, DocIdRangeVector& ranges);
    static bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, const index::PartitionInfoPtr& partitionInfo,
                                     size_t totalWayCount, std::vector<DocIdRangeVector>& rangeVectors);

private:
    static bool InRange(const DocIdRange& inner, const DocIdRange& outter)
    {
        return inner.first >= outter.first && inner.second <= outter.second;
    }

    static DocIdRangeVector::const_iterator IntersectRange(const DocIdRangeVector::const_iterator& begin1,
                                                           const DocIdRangeVector::const_iterator& end1,
                                                           const DocIdRangeVector::const_iterator& begin2,
                                                           const DocIdRangeVector::const_iterator& end2,
                                                           DocIdRangeVector& output);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocRangePartitioner);
}} // namespace indexlib::partition

#endif //__INDEXLIB_DOC_RANGE_PARTITIONER_H
