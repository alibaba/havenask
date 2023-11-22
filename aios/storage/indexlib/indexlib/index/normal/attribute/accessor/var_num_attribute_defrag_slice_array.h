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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_offset_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index/normal/attribute/format/updatable_var_num_attribute_offset_formatter.h"
#include "indexlib/index/normal/attribute/format/var_num_attribute_data_formatter.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/slice_array/DefragSliceArray.h"

namespace indexlib { namespace index {

class VarNumAttributeDefragSliceArray : public util::DefragSliceArray
{
public:
    VarNumAttributeDefragSliceArray(const util::DefragSliceArray::SliceArrayPtr& sliceArray,
                                    const std::string& attrName, uint64_t defragPercentThreshold);
    ~VarNumAttributeDefragSliceArray();

public:
    void Init(AttributeOffsetReader* offsetReader, const UpdatableVarNumAttributeOffsetFormatter& offsetFormatter,
              const VarNumAttributeDataFormatter& dataFormatter, AttributeMetrics* attributeMetrics);

private:
    void DoFree(size_t size) override;
    void Defrag(int64_t sliceIdx) override;
    bool NeedDefrag(int64_t sliceIdx) override;

    void MoveData(docid_t docId, uint64_t offset) __ALWAYS_INLINE;

    void InitMetrics(AttributeMetrics* attributeMetrics);

private:
    UpdatableVarNumAttributeOffsetFormatter mOffsetFormatter;
    VarNumAttributeDataFormatter mDataFormatter;
    AttributeOffsetReader* mOffsetReader;
    AttributeMetrics* mAttributeMetrics;
    std::vector<int64_t> mUselessSliceIdxs;
    std::string mAttrName;
    uint64_t mDefragPercentThreshold;

private:
    friend class VarNumAttributeDefragSliceArrayTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarNumAttributeDefragSliceArray);
//////////////////////////////////////////////////////////////////////
inline void VarNumAttributeDefragSliceArray::MoveData(docid_t docId, uint64_t offset)
{
    uint64_t sliceOffset = mOffsetFormatter.DecodeToSliceArrayOffset(offset);
    const void* data = Get(sliceOffset);
    bool isNull = false;
    size_t size = mDataFormatter.GetDataLength((uint8_t*)data, isNull);
    uint64_t newSliceOffset = Append(data, size);
    uint64_t newOffset = mOffsetFormatter.EncodeSliceArrayOffset(newSliceOffset);
    mOffsetReader->SetOffset(docId, newOffset);
}
}} // namespace indexlib::index
