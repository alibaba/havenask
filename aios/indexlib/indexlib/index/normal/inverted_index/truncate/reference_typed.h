#ifndef __INDEXLIB_REFERENCE_TYPED_H
#define __INDEXLIB_REFERENCE_TYPED_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/reference.h"

IE_NAMESPACE_BEGIN(index);

template <typename T>
class ReferenceTyped : public Reference
{
public:
    ReferenceTyped(size_t offset, FieldType fieldType)
        : Reference(offset, fieldType)
    {
    }

    virtual ~ReferenceTyped() {};

public:
    void Get(const DocInfo* docInfo, T &value);
    void Set(const T& value, DocInfo* docInfo);
    std::string GetStringValue(DocInfo* docInfo) override;

private:
    IE_LOG_DECLARE();
};


//////////////////////////////////////////////////////////////////////
template <typename T>
void ReferenceTyped<T>::Get(const DocInfo* docInfo, T &value)
{
    value = *(T *)(docInfo->Get(mOffset));
}

template <typename T>
void ReferenceTyped<T>::Set(const T& value, DocInfo* docInfo)
{
    uint8_t* buffer = docInfo->Get(mOffset);
    *((T*)buffer) = value;
}

template <typename T>
std::string ReferenceTyped<T>::GetStringValue(DocInfo* docInfo)
{
    T value;
    Get(docInfo, value);
    return autil::StringUtil::toString(value);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_REFERENCE_TYPED_H
