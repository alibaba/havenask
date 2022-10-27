#ifndef __INDEXLIB_SEARCH_SUMMARY_DOCUMENT_H
#define __INDEXLIB_SEARCH_SUMMARY_DOCUMENT_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include <autil/mem_pool/PoolBase.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(document);

class SearchSummaryDocument
{
public:
    SearchSummaryDocument(autil::mem_pool::PoolBase* pool, size_t size);
    ~SearchSummaryDocument();
public:
    //for deserialize summary hit
    void UpdateFieldCount(size_t size);
    char* AllocateFromBuffer(size_t size);
    autil::mem_pool::PoolBase* getPool();
public:
    bool SetFieldValue(int32_t summaryFieldId, const char* fieldValue,
                       size_t size, bool needCopy = true);
    const autil::ConstString* GetFieldValue(int32_t summaryFieldId) const;
    
    void ClearFieldValue(int32_t summaryFieldId);

    size_t GetFieldCount() const { return mFieldValues.size(); }

    char* GetBuffer(size_t size);

    const std::vector<autil::ConstString*> &GetFields() const { return mFieldValues; }
private:
    bool mOwnPool;
    autil::mem_pool::PoolBase* mPool;
    std::vector<autil::ConstString*> mFieldValues;
    char* mBuffer;
    size_t mBufferUsed;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SearchSummaryDocument);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SEARCH_SUMMARY_DOCUMENT_H
