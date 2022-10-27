#ifndef __INDEXLIB_DOC_INFO_ALLOCATOR_H
#define __INDEXLIB_DOC_INFO_ALLOCATOR_H

#include <tr1/memory>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/mem_pool/ChunkAllocatorBase.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/doc_info.h"
#include "indexlib/index/normal/inverted_index/truncate/reference_typed.h"

IE_NAMESPACE_BEGIN(index);

class DocInfoAllocator
{
public:
    static const std::string DOCID_REFER_NAME;
public:
    DocInfoAllocator();
    virtual ~DocInfoAllocator();

public:
    template <typename T>
    ReferenceTyped<T>* DeclareReferenceTyped(const std::string& fieldName,
            FieldType fieldType);

    Reference* DeclareReference(const std::string& fieldName,
                                FieldType fieldType);

    Reference* GetReference(const std::string& fieldName) const;

    DocInfo* Allocate();
    void Deallocate(DocInfo *docInfo);

    uint32_t GetDocInfoSize() const { return mDocInfoSize; }

    void Release();
private:
    template <typename T>
    ReferenceTyped<T>* CreateReference(const std::string& fieldName,
            FieldType fieldType);

private:
    typedef std::map<std::string, Reference*> ReferenceMap;
    ReferenceMap mRefers;
    uint32_t mDocInfoSize;
    autil::mem_pool::ChunkAllocatorBase* mAllocator;
    autil::mem_pool::RecyclePool* mBufferPool;

private:
    IE_LOG_DECLARE();
};

template <typename T>
inline ReferenceTyped<T>* DocInfoAllocator::CreateReference(
        const std::string& fieldName, 
        FieldType fieldType)
{
    ReferenceTyped<T> *refer = 
        new ReferenceTyped<T>(mDocInfoSize, fieldType);
    mRefers[fieldName] = refer;
    mDocInfoSize += sizeof(T);
    return refer;
}

template <typename T>
inline ReferenceTyped<T>* DocInfoAllocator::DeclareReferenceTyped(
        const std::string& fieldName, 
        FieldType fieldType)
{
    Reference* refer = DeclareReference(fieldName, fieldType);
    ReferenceTyped<T> *referTyped = dynamic_cast<ReferenceTyped<T>* >(refer);
    assert(referTyped);
    return referTyped;
}

inline Reference* DocInfoAllocator::DeclareReference(
        const std::string& fieldName, 
        FieldType fieldType)
{
    Reference* refer = GetReference(fieldName);
    if (refer)
    {
        return refer;
    }

    switch (fieldType)
    {
    case ft_int8:
        return CreateReference<int8_t>(fieldName, fieldType);
    case ft_uint8:
        return CreateReference<uint8_t>(fieldName, fieldType);
    case ft_int16:
        return CreateReference<int16_t>(fieldName, fieldType);
    case ft_uint16:
        return CreateReference<uint16_t>(fieldName, fieldType);
    case ft_int32:
        return CreateReference<int32_t>(fieldName, fieldType);
    case ft_uint32:
        return CreateReference<uint32_t>(fieldName, fieldType);
    case ft_int64:
        return CreateReference<int64_t>(fieldName, fieldType);
    case ft_uint64:
        return CreateReference<uint64_t>(fieldName, fieldType);
    case ft_float:
        return CreateReference<float>(fieldName, fieldType);
    case ft_fp8:
        return CreateReference<int8_t>(fieldName, fieldType);
    case ft_fp16:
        return CreateReference<int16_t>(fieldName, fieldType);
    case ft_double:
        return CreateReference<double>(fieldName, fieldType);
    default:
        assert(false);
    }

    return refer;
}

inline Reference* DocInfoAllocator::GetReference(const std::string& fieldName) const
{
    ReferenceMap::const_iterator it = mRefers.find(fieldName);
    if (it == mRefers.end())
    {
        return NULL;
    }
    return it->second;
}

DEFINE_SHARED_PTR(DocInfoAllocator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_INFO_ALLOCATOR_H
