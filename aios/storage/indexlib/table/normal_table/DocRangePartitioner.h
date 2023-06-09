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
#pragma once
#include <memory>
#include <vector>

#include "autil/Log.h"
#include "indexlib/base/Types.h"

namespace indexlibv2 { namespace table {

class DocRangePartitioner
{
public:
    static bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t incDocCount, size_t totalWayCount,
                                     size_t wayIdx, DocIdRangeVector& ranges);
    static bool GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t incDocCount, size_t totalWayCount,
                                     std::vector<DocIdRangeVector>& rangeVectors);

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
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::table
