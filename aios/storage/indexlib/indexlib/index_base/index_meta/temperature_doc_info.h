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
#ifndef __INDEXLIB_TEMPERATURE_DOC_INFO_H
#define __INDEXLIB_TEMPERATURE_DOC_INFO_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_define.h"

namespace indexlib { namespace index_base {

class TemperatureDocInfo
{
public:
    TemperatureDocInfo();
    ~TemperatureDocInfo();

    TemperatureDocInfo(const std::map<index::TemperatureProperty, DocIdRangeVector>& meta) : mTemperatureDocRange(meta)
    {
    }

    TemperatureDocInfo(const TemperatureDocInfo& other) : mTemperatureDocRange(other.mTemperatureDocRange) {}

public:
    void Init(const index_base::Version& version, const index_base::SegmentDataVector& segmentDatas,
              const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments, docid_t totalDocCount);
    bool GetTemperatureDocIdRanges(int32_t hintValues, DocIdRangeVector& ranges) const;
    void AddNewSegmentInfo(index::TemperatureProperty property, segmentid_t segId, docid_t startDocId,
                           docid_t endDocId);
    bool IsEmptyInfo() const { return mTemperatureDocRange.empty(); }
    index::TemperatureProperty GetTemperaturePropery(segmentid_t segId);

private:
    DocIdRangeVector CombineDocIdRange(const DocIdRangeVector& currentRange, const DocIdRangeVector& range) const;
    void InitBaseDocIds(const index_base::SegmentDataVector& segmentDatas,
                        const std::vector<index_base::InMemorySegmentPtr>& dumpingSegments,
                        std::map<segmentid_t, docid_t>& segIdToBaseDocId);

private:
    std::map<index::TemperatureProperty, DocIdRangeVector> mTemperatureDocRange;
    std::map<segmentid_t, index::TemperatureProperty> mTemperatureSegInfo;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TemperatureDocInfo);

}} // namespace indexlib::index_base

#endif //__INDEXLIB_TEMPERATURE_DOC_INFO_H
