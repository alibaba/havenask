#ifndef __INDEXLIB_MULTI_VALUE_H
#define __INDEXLIB_MULTI_VALUE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/atomic_value.h"

IE_NAMESPACE_BEGIN(common);

class MultiValue
{
public:
    MultiValue();
    ~MultiValue();
public:
    const AtomicValueVector& GetAtomicValues() const
    { return mAtomicValues; }

    AtomicValue* GetAtomicValue(size_t index) const
    { 
        assert(index < mAtomicValues.size()); 
        return mAtomicValues[index]; 
    }

    size_t GetAtomicValueSize() const
    { return mAtomicValues.size(); }

    size_t GetTotalSize() const
    { return mSize; }

    void AddAtomicValue(AtomicValue* atomicValue)
    {
        assert(atomicValue);
        mSize += atomicValue->GetSize();
        mAtomicValues.push_back(atomicValue);
    }

protected:
    AtomicValueVector mAtomicValues;
    uint32_t mSize;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiValue);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_MULTI_VALUE_H
