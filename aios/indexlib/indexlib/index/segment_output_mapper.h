#ifndef __INDEXLIB_SEGMENT_OUTPUT_MAPPER_H
#define __INDEXLIB_SEGMENT_OUTPUT_MAPPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/merger_resource.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include <functional>

IE_NAMESPACE_BEGIN(index);

template<typename T>
class SegmentOutputMapper
{
public:
    SegmentOutputMapper() {}

    void Init(ReclaimMapPtr reclaimMap,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
        std::function<T(const index_base::OutputSegmentMergeInfo& outputInfo, size_t outputIdx)>
            outputCreateFunc)

    {
        mReclaimMap = reclaimMap;
        mSegIdx2OutputIdx.resize(MAX_SPLIT_SEGMENT_COUNT, -1);
        for (size_t i = 0; i < outputSegMergeInfos.size(); ++i)
        {
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
        if (outputIdx == -1)
        {
            return nullptr;
        }
        return &mOutputs[outputIdx];
    }

    T* GetOutputBySegIdx(segmentindex_t segIdx)
    {
        assert(segIdx < mSegIdx2OutputIdx.size());
        auto outputIdx = mSegIdx2OutputIdx[segIdx];
        if (outputIdx == -1)
        {
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

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_OUTPUT_MAPPER_H
