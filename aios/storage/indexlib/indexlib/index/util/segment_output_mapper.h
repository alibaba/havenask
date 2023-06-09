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
#ifndef __INDEXLIB_SEGMENT_OUTPUT_MAPPER_H
#define __INDEXLIB_SEGMENT_OUTPUT_MAPPER_H

#include <functional>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T>
class SegmentOutputMapper
{
public:
    SegmentOutputMapper() {}

    void Init(ReclaimMapPtr reclaimMap, const std::vector<std::pair<T, segmentindex_t>>& outputInfos)
    {
        mReclaimMap = reclaimMap;
        mSegIdx2OutputIdx.resize(MAX_SPLIT_SEGMENT_COUNT, -1);
        for (size_t i = 0; i < outputInfos.size(); ++i) {
            mOutputs.push_back(outputInfos[i].first);
            mSegIdx2OutputIdx[outputInfos[i].second] = i;
        }
    }

    void Init(ReclaimMapPtr reclaimMap, const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
              std::function<T(const index_base::OutputSegmentMergeInfo& outputInfo, size_t outputIdx)> outputCreateFunc)

    {
        mReclaimMap = reclaimMap;
        mSegIdx2OutputIdx.resize(MAX_SPLIT_SEGMENT_COUNT, -1);
        for (size_t i = 0; i < outputSegMergeInfos.size(); ++i) {
            mOutputs.push_back(outputCreateFunc(outputSegMergeInfos[i], i));
            const auto& outputInfo = outputSegMergeInfos[i];
            mSegIdx2OutputIdx[outputInfo.targetSegmentIndex] = i;
        }
    }

    T* GetOutput(docid_t docIdInPlan)
    {
        auto targetSegIdx = mReclaimMap->GetTargetSegmentIndex(docIdInPlan);
        assert(targetSegIdx < mSegIdx2OutputIdx.size());
        auto outputIdx = mSegIdx2OutputIdx[targetSegIdx];
        if (outputIdx == -1) {
            return nullptr;
        }
        return &mOutputs[outputIdx];
    }

    T* GetOutputBySegIdx(segmentindex_t segIdx)
    {
        assert(segIdx < mSegIdx2OutputIdx.size());
        auto outputIdx = mSegIdx2OutputIdx[segIdx];
        if (outputIdx == -1) {
            return nullptr;
        }
        return &mOutputs[outputIdx];
    }

    const ReclaimMapPtr& GetReclaimMap() const { return mReclaimMap; }

    std::vector<T>& GetOutputs() { return mOutputs; }

    void Clear()
    {
        mReclaimMap.reset();
        mSegIdx2OutputIdx.clear();
        mOutputs.clear();
    }

private:
    ReclaimMapPtr mReclaimMap;
    std::vector<int32_t> mSegIdx2OutputIdx;
    std::vector<T> mOutputs;
};
}} // namespace indexlib::index

#endif //__INDEXLIB_SEGMENT_OUTPUT_MAPPER_H
