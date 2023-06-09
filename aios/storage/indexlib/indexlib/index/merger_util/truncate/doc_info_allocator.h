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
#ifndef __INDEXLIB_DOC_INFO_ALLOCATOR_H
#define __INDEXLIB_DOC_INFO_ALLOCATOR_H

#include <memory>

#include "autil/mem_pool/ChunkAllocatorBase.h"
#include "autil/mem_pool/RecyclePool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/doc_info.h"
#include "indexlib/index/merger_util/truncate/reference_typed.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class DocInfoAllocator
{
public:
    static const std::string DOCID_REFER_NAME;

public:
    DocInfoAllocator();
    virtual ~DocInfoAllocator();

public:
    template <typename T>
    ReferenceTyped<T>* DeclareReferenceTyped(const std::string& fieldName, FieldType fieldType, bool supportNull);

    Reference* DeclareReference(const std::string& fieldName, FieldType fieldType, bool supportNull);

    Reference* GetReference(const std::string& fieldName) const;

    DocInfo* Allocate();
    void Deallocate(DocInfo* docInfo);

    uint32_t GetDocInfoSize() const { return mDocInfoSize; }

    void Release();

private:
    template <typename T>
    ReferenceTyped<T>* CreateReference(const std::string& fieldName, FieldType fieldType, bool supportNull);

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
inline ReferenceTyped<T>* DocInfoAllocator::CreateReference(const std::string& fieldName, FieldType fieldType,
                                                            bool supportNull)
{
    ReferenceTyped<T>* refer = new ReferenceTyped<T>(mDocInfoSize, fieldType, supportNull);
    mRefers[fieldName] = refer;
    mDocInfoSize += refer->GetRefSize();
    return refer;
}

template <typename T>
inline ReferenceTyped<T>* DocInfoAllocator::DeclareReferenceTyped(const std::string& fieldName, FieldType fieldType,
                                                                  bool supportNull)
{
    Reference* refer = DeclareReference(fieldName, fieldType, supportNull);
    ReferenceTyped<T>* referTyped = dynamic_cast<index::legacy::ReferenceTyped<T>*>(refer);
    assert(referTyped);
    return referTyped;
}

inline Reference* DocInfoAllocator::DeclareReference(const std::string& fieldName, FieldType fieldType,
                                                     bool supportNull)
{
    Reference* refer = GetReference(fieldName);
    if (refer) {
        return refer;
    }

    switch (fieldType) {
    case ft_int8:
        return CreateReference<int8_t>(fieldName, fieldType, supportNull);
    case ft_uint8:
        return CreateReference<uint8_t>(fieldName, fieldType, supportNull);
    case ft_int16:
        return CreateReference<int16_t>(fieldName, fieldType, supportNull);
    case ft_uint16:
        return CreateReference<uint16_t>(fieldName, fieldType, supportNull);
    case ft_int32:
        return CreateReference<int32_t>(fieldName, fieldType, supportNull);
    case ft_uint32:
        return CreateReference<uint32_t>(fieldName, fieldType, supportNull);
    case ft_int64:
        return CreateReference<int64_t>(fieldName, fieldType, supportNull);
    case ft_uint64:
        return CreateReference<uint64_t>(fieldName, fieldType, supportNull);
    case ft_float:
        return CreateReference<float>(fieldName, fieldType, supportNull);
    case ft_fp8:
        return CreateReference<int8_t>(fieldName, fieldType, supportNull);
    case ft_fp16:
        return CreateReference<int16_t>(fieldName, fieldType, supportNull);
    case ft_double:
        return CreateReference<double>(fieldName, fieldType, supportNull);
    case ft_date:
        return CreateReference<uint32_t>(fieldName, fieldType, supportNull);
    case ft_time:
        return CreateReference<uint32_t>(fieldName, fieldType, supportNull);
    case ft_timestamp:
        return CreateReference<uint64_t>(fieldName, fieldType, supportNull);
    default:
        assert(false);
    }

    return refer;
}

inline Reference* DocInfoAllocator::GetReference(const std::string& fieldName) const
{
    ReferenceMap::const_iterator it = mRefers.find(fieldName);
    if (it == mRefers.end()) {
        return NULL;
    }
    return it->second;
}

DEFINE_SHARED_PTR(DocInfoAllocator);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_DOC_INFO_ALLOCATOR_H
