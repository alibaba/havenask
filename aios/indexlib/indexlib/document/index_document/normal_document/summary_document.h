#ifndef __INDEXLIB_SUMMARY_DOCUMENT_H
#define __INDEXLIB_SUMMARY_DOCUMENT_H

#include <tr1/memory>
#include <vector>
#include <string>
#include <autil/ConstString.h>
#include <autil/DataBuffer.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

IE_NAMESPACE_BEGIN(document);

class SummaryDocument
{
public:
    typedef std::vector<autil::ConstString> FieldVector;

public:
    class Iterator
    {
    public:
        Iterator(const FieldVector& fieldVec, 
                 fieldid_t firstNotEmptyFieldId = INVALID_FIELDID) 
            : mFields(fieldVec)
        {
            mCursor = mFields.begin();
            if (firstNotEmptyFieldId != INVALID_FIELDID)
            {
                assert((size_t)firstNotEmptyFieldId < mFields.size());
                mFieldId = firstNotEmptyFieldId;
                mCursor += mFieldId;
            }
            else
            {
                mFieldId = 0;
                MoveToNext();
            }
        }

    public:
        bool HasNext() const 
        {
            return mCursor != mFields.end(); 
        }
        
        const autil::ConstString& Next(fieldid_t& fieldId)
        {
            if (mCursor != mFields.end())
            {
                const autil::ConstString& value = *mCursor;
                fieldId = mFieldId;
                ++mCursor;
                ++mFieldId;
                MoveToNext();
                return value;
            }
            return autil::ConstString::EMPTY_STRING;
        }

    private:
        void MoveToNext()
        {
            while(mCursor != mFields.end() && mCursor->empty())
            {
                ++mCursor;
                ++mFieldId;
            }
        }

    private:
        const FieldVector& mFields;
        FieldVector::const_iterator mCursor;
        fieldid_t mFieldId;
    };

public:
    SummaryDocument(autil::mem_pool::Pool* pool = NULL)
        : mNotEmptyFieldCount(0)
        , mFirstNotEmptyFieldId(INVALID_FIELDID)
        , mDocId(INVALID_DOCID)
    {}
    
    ~SummaryDocument()
    {}

public:
    void SetDocId(docid_t docId){mDocId = docId; }
    docid_t GetDocId() const { return mDocId; }
    // caller: BS::ClassifiedDocument
    void SetField(fieldid_t id, const autil::ConstString& value);
    void ClearFields(const std::vector<fieldid_t>& fieldIds);
    bool HasField(fieldid_t id) const;
    const autil::ConstString& GetField(fieldid_t id) const;

    uint32_t GetNotEmptyFieldCount() const { return mNotEmptyFieldCount; }
    void Reserve(size_t size) {  mFields.resize(size); }
    void Reset()
    {
        mFields.clear();
        mNotEmptyFieldCount = 0;
        mFirstNotEmptyFieldId = INVALID_FIELDID;
    }

    Iterator CreateIterator() const
    { return Iterator(mFields, mFirstNotEmptyFieldId); }

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::Pool *pool);

    void deserializeLegacyFormat(autil::DataBuffer &dataBuffer,
                                 autil::mem_pool::Pool *pool, uint32_t docVersion);
    
    bool operator == (const SummaryDocument& right) const;
    bool operator != (const SummaryDocument& right) const;

private:
    bool NeedPartialSerialize() const;
    void DeserializeAllFields(autil::DataBuffer &dataBuffer,
                              autil::mem_pool::Pool *pool);
    void DeserializeNotEmptyFields(autil::DataBuffer &dataBuffer,
                                   autil::mem_pool::Pool *pool);
private:
    FieldVector mFields;
    uint32_t mNotEmptyFieldCount;
    fieldid_t mFirstNotEmptyFieldId;
    docid_t mDocId;

private:
    IE_LOG_DECLARE();
private:
    friend class SummaryDocumentTest;
    friend class DocumentTest;
};

DEFINE_SHARED_PTR(SummaryDocument);
//////////////////////////////////////////////////////////////////
inline bool SummaryDocument::HasField(fieldid_t id) const
{
    if (id < (fieldid_t)mFields.size())
    {
        return !mFields[id].empty();
    }

    return false;
}

inline void SummaryDocument::SetField(
        fieldid_t id, const autil::ConstString& value)
{
    if (id >= (fieldid_t)mFields.size())
    {
        mFields.resize(id + 1);
    }
    
    if (mFields[id].empty() && !value.empty())
    {
        ++mNotEmptyFieldCount;
        if (mFirstNotEmptyFieldId == INVALID_FIELDID || id < mFirstNotEmptyFieldId)
        {
            mFirstNotEmptyFieldId = id;
        }
    } 
    mFields[id] = value;
}

inline bool SummaryDocument::NeedPartialSerialize() const
{
    if (mFields.size() == (size_t)mNotEmptyFieldCount)
    {
        // all fields not empty
        return false;
    }

    size_t maxVintSize = 0;
    uint32_t maxFieldId = mFields.size();
    while (maxFieldId > 0x7F)
    {
        maxFieldId >>= 7;
        ++maxVintSize;
    }
    ++maxVintSize;

    // each empty field use 1 byte
    size_t emptyFieldSize = mFields.size() - mNotEmptyFieldCount;
    return emptyFieldSize > (maxVintSize * mNotEmptyFieldCount);
}

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SUMMARY_DOCUMENT_H
