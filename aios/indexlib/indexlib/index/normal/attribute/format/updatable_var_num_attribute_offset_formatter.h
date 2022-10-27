#ifndef __INDEXLIB_UPDATABLE_VAR_NUM_ATTRIBUTE_OFFSET_FORMATTER_H
#define __INDEXLIB_UPDATABLE_VAR_NUM_ATTRIBUTE_OFFSET_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class UpdatableVarNumAttributeOffsetFormatter
{
public:
    UpdatableVarNumAttributeOffsetFormatter();
    ~UpdatableVarNumAttributeOffsetFormatter();

public:
    void Init(uint64_t dataSize)
    { mDataSize = dataSize; }

    bool IsSliceArrayOffset(uint64_t offset) const __ALWAYS_INLINE;
    uint64_t EncodeSliceArrayOffset(uint64_t offset) const __ALWAYS_INLINE;
    uint64_t DecodeToSliceArrayOffset(uint64_t offset) const __ALWAYS_INLINE;

private:
    uint64_t mDataSize;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UpdatableVarNumAttributeOffsetFormatter);

///////////////////////////////////////////////////////////////////////
inline bool UpdatableVarNumAttributeOffsetFormatter::IsSliceArrayOffset(
        uint64_t offset) const
{
    return offset >= mDataSize;
}

inline uint64_t UpdatableVarNumAttributeOffsetFormatter::EncodeSliceArrayOffset(
        uint64_t originalOffset) const
{
    return originalOffset + mDataSize;
}

inline uint64_t UpdatableVarNumAttributeOffsetFormatter::DecodeToSliceArrayOffset(
        uint64_t offset) const
{
    return offset - mDataSize;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_UPDATABLE_VAR_NUM_ATTRIBUTE_OFFSET_FORMATTER_H
