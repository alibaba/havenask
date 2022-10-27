#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_DEFRAG_SLICE_ARRAY_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_DEFRAG_SLICE_ARRAY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_offset_reader.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index/normal/attribute/format/updatable_var_num_attribute_offset_formatter.h"
#include "indexlib/index/normal/attribute/format/var_num_attribute_data_formatter.h"
#include "indexlib/util/slice_array/defrag_slice_array.h"

IE_NAMESPACE_BEGIN(index);

class VarNumAttributeDefragSliceArray : public util::DefragSliceArray
{
public:
    VarNumAttributeDefragSliceArray(const util::DefragSliceArray::SliceArrayPtr& sliceArray,
                                    const std::string& attrName,
                                    float defragPercentThreshold);
    ~VarNumAttributeDefragSliceArray();

public:
    void Init(AttributeOffsetReader* offsetReader, 
              const UpdatableVarNumAttributeOffsetFormatter& offsetFormatter,
              const VarNumAttributeDataFormatter& dataFormatter,
              AttributeMetrics* attributeMetrics);

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
    float mDefragPercentThreshold;
    
private:
    friend class VarNumAttributeDefragSliceArrayTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarNumAttributeDefragSliceArray);
//////////////////////////////////////////////////////////////////////
inline void VarNumAttributeDefragSliceArray::MoveData(
        docid_t docId, uint64_t offset)
{
    uint64_t sliceOffset = mOffsetFormatter.DecodeToSliceArrayOffset(offset);
    const void* data = Get(sliceOffset);
    size_t size = mDataFormatter.GetDataLength((uint8_t*)data);
    uint64_t newSliceOffset = Append(data, size);
    uint64_t newOffset = mOffsetFormatter.EncodeSliceArrayOffset(newSliceOffset);
    mOffsetReader->SetOffset(docId, newOffset);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_DEFRAG_SLICE_ARRAY_H
