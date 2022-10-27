#include "indexlib/document/index_document/normal_document/search_summary_document.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, SearchSummaryDocument);
static const size_t DEFAULT_SUMMARY_DOC_SIZE = 2048;

SearchSummaryDocument::SearchSummaryDocument(PoolBase *pool, size_t size)
    : mOwnPool(pool == NULL)
    , mPool(pool ? pool : new Pool())
    , mBuffer(NULL)
    , mBufferUsed(0)

{
    mFieldValues.resize(size, NULL);
}

SearchSummaryDocument::~SearchSummaryDocument()
{
    if (mOwnPool)
    {
        delete mPool;
        mPool = NULL;
    }
}

char* SearchSummaryDocument::GetBuffer(size_t size)
{
    return static_cast<char*>(mPool->allocate(size));
}

void SearchSummaryDocument::UpdateFieldCount(size_t size) {
    if (mFieldValues.size() < size) {
        mFieldValues.resize(size, NULL);
    }
}

char* SearchSummaryDocument::AllocateFromBuffer(size_t size)
{
    if (mBuffer == NULL) {
        mBuffer = (char*)mPool->allocate(DEFAULT_SUMMARY_DOC_SIZE);
        if (mBuffer == NULL) {
            return NULL;
        }
        mBufferUsed = 0;
    } 

    if (size + mBufferUsed >= DEFAULT_SUMMARY_DOC_SIZE) {
        return (char*)mPool->allocate(size);
    } else {
        char* start = mBuffer + mBufferUsed;
        mBufferUsed += size;
        return start;
    }
}

autil::mem_pool::PoolBase* SearchSummaryDocument::getPool() {
    return mPool;
}

bool SearchSummaryDocument::SetFieldValue(
        int32_t summaryFieldId, const char* fieldValue,
        size_t size, bool needCopy)
{
    assert(summaryFieldId >= 0);
    if (summaryFieldId >= (int32_t)mFieldValues.size())
    {
        mFieldValues.resize(summaryFieldId + 1, NULL);
    }
    void* constStringPlacement = (void*)AllocateFromBuffer(sizeof(ConstString));
    if (NULL == constStringPlacement)
    {
        return false;
    }
    const char* value = fieldValue;
    if (needCopy)
    {
        char* tmpValue = AllocateFromBuffer(size);
        if (NULL == tmpValue)
        {
            return false;
        }
        memcpy(tmpValue, fieldValue, size);
        value = tmpValue;
    }
    mFieldValues[summaryFieldId] = new (constStringPlacement) ConstString(value, size);
    return true;
}

const ConstString* SearchSummaryDocument::GetFieldValue(int32_t summaryFieldId) const
{
    if (summaryFieldId < 0 || summaryFieldId >= (int32_t)mFieldValues.size())
    {
        return NULL;
    }
    return mFieldValues[summaryFieldId];
}

void SearchSummaryDocument::ClearFieldValue(int32_t summaryFieldId)
{
    assert(summaryFieldId >= 0);
    if (summaryFieldId >= (int32_t)mFieldValues.size())
    {
        mFieldValues.resize(summaryFieldId + 1, NULL);
    }
    mFieldValues[summaryFieldId] = NULL;
}

IE_NAMESPACE_END(document);

