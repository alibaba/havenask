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
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_defrag_slice_array.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarNumAttributeDefragSliceArray);

VarNumAttributeDefragSliceArray::VarNumAttributeDefragSliceArray(const DefragSliceArray::SliceArrayPtr& sliceArray,
                                                                 const string& attrName,
                                                                 uint64_t defragPercentThreshold)
    : DefragSliceArray(sliceArray)
    , mOffsetReader(NULL)
    , mAttributeMetrics(NULL)
    , mAttrName(attrName)
    , mDefragPercentThreshold(defragPercentThreshold)
{
}

VarNumAttributeDefragSliceArray::~VarNumAttributeDefragSliceArray()
{
    for (size_t i = 0; i < mUselessSliceIdxs.size(); i++) {
        ReleaseSlice(mUselessSliceIdxs[i]);
        IE_LOG(DEBUG, "release attribute [%s] slice [%ld]", mAttrName.c_str(), mUselessSliceIdxs[i]);
    }

    if (mAttributeMetrics) {
        mAttributeMetrics->IncreaseReclaimedSliceCountValue(mUselessSliceIdxs.size());

        int64_t releaseMemSize = GetSliceLen() * mUselessSliceIdxs.size();
        mAttributeMetrics->IncreaseTotalReclaimedBytesValue(releaseMemSize);
    }
}

void VarNumAttributeDefragSliceArray::Init(AttributeOffsetReader* offsetReader,
                                           const UpdatableVarNumAttributeOffsetFormatter& offsetFormatter,
                                           const VarNumAttributeDataFormatter& dataFormatter,
                                           AttributeMetrics* attributeMetrics)
{
    mOffsetReader = offsetReader;
    mOffsetFormatter = offsetFormatter;
    mDataFormatter = dataFormatter;
    mAttributeMetrics = attributeMetrics;
    InitMetrics(mAttributeMetrics);
}

void VarNumAttributeDefragSliceArray::Defrag(int64_t sliceIdx)
{
    IE_LOG(DEBUG, "Begin defrag attribute [%s], sliceIdx [%ld]", mAttrName.c_str(), sliceIdx);

    int64_t sliceLen = GetSliceLen();
    // [Begin, End)
    uint64_t offsetBegin = mOffsetFormatter.EncodeSliceArrayOffset(SliceIdxToOffset(sliceIdx));
    uint64_t offsetEnd = offsetBegin + sliceLen;
    docid_t docCount = mOffsetReader->GetDocCount();
    for (docid_t docId = 0; docId < docCount; docId++) {
        uint64_t offset = mOffsetReader->GetOffset(docId);
        if (offset >= offsetBegin && offset < offsetEnd) {
            MoveData(docId, offset);
        }
    }

    if (mAttributeMetrics) {
        mAttributeMetrics->IncreaseCurReaderReclaimableBytesValue(sliceLen);
        int64_t movedSize = sliceLen - GetWastedSize(sliceIdx) - GetHeaderSize();
        mAttributeMetrics->IncreaseVarAttributeWastedBytesValue(movedSize);
    }

    SetWastedSize(sliceIdx, sliceLen);
    mUselessSliceIdxs.push_back(sliceIdx);

    IE_LOG(DEBUG, "End defrag attribute [%s], sliceIdx [%ld]", mAttrName.c_str(), sliceIdx);
}

void VarNumAttributeDefragSliceArray::DoFree(size_t size)
{
    if (mAttributeMetrics) {
        mAttributeMetrics->IncreaseVarAttributeWastedBytesValue(size);
    }
}

bool VarNumAttributeDefragSliceArray::NeedDefrag(int64_t sliceIdx)
{
    if (sliceIdx == GetCurrentSliceIdx()) {
        return false;
    }

    size_t wastedSize = GetWastedSize(sliceIdx);
    size_t threshold = (size_t)(GetSliceLen() * (mDefragPercentThreshold / 100.0));

    return wastedSize >= threshold;
}

void VarNumAttributeDefragSliceArray::InitMetrics(AttributeMetrics* attributeMetrics)
{
    if (!attributeMetrics) {
        return;
    }

    int64_t sliceNum = GetSliceNum();
    uint64_t sliceLen = GetSliceLen();
    int64_t totalWastedSize = 0;
    for (int64_t i = 0; i < sliceNum; ++i) {
        int32_t wastedSize = GetWastedSize(i);
        if (wastedSize != (int64_t)sliceLen) {
            totalWastedSize += wastedSize;
        }
    }
    attributeMetrics->IncreaseVarAttributeWastedBytesValue(totalWastedSize);
}
}} // namespace indexlib::index
