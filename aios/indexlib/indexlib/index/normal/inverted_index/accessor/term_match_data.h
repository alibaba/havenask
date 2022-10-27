#ifndef __INDEXLIB_TERM_MATCH_DATA_H
#define __INDEXLIB_TERM_MATCH_DATA_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_state.h"

DECLARE_REFERENCE_CLASS(index, InDocPositionIterator);

IE_NAMESPACE_BEGIN(index);

class TermMatchData
{
public:
    struct IndexInfo
    {
        uint8_t hasDocPayload:1;
        uint8_t hasFieldMap:1;
        uint8_t hasFirstOcc:1;
        uint8_t isMatched:1;
        uint8_t unused:4;

        IndexInfo()
        {
            *((uint8_t *)this) = 0;
        }

        inline bool HasDocPayload() const { return hasDocPayload == 1; }

        inline void SetDocPayload(bool flag) { hasDocPayload = flag ? 1 : 0; }

        inline bool HasFieldMap() const { return hasFieldMap == 1; }

        inline void SetFieldMap(bool flag) { hasFieldMap = flag ? 1 : 0; }

        inline bool HasFirstOcc() const { return hasFirstOcc == 1; }

        inline void SetFirstOcc(bool flag) { hasFirstOcc = flag ? 1 : 0; }

        inline bool IsMatched() const { return isMatched == 1; }

        inline void SetMatched(bool flag) { isMatched = flag ? 1 : 0; }
    };

public:
    TermMatchData() 
        : mPosState(NULL)
        , mFirstOcc(0)
        , mDocPayload(0)
        , mFieldMap(0)
    {
    }

    ~TermMatchData() { }
    
public:
    tf_t GetTermFreq() const 
    {  
        return  mPosState != NULL ? mPosState->GetTermFreq() : 1;
    }

    pos_t GetFirstOcc() const { return mFirstOcc; }
    
    fieldmap_t GetFieldMap() const { return mFieldMap; }

    docpayload_t GetDocPayload() const
    {
        if (!mIndexInfo.HasDocPayload())
        {
            return 0;
        }
        return mDocPayload;
    }

    InDocPositionState* GetInDocPositionState() const { return mPosState; }

    void SetInDocPositionState(InDocPositionState* state)
    {
        mPosState = state;
        SetMatched(mPosState != NULL);
    }

    void FreeInDocPositionState()
    {
        if (mPosState)
        {
            mPosState->Free();
            mPosState = NULL;
        }
    }

    InDocPositionIteratorPtr GetInDocPositionIterator() const
    {
        if (mPosState)
        {
            return InDocPositionIteratorPtr(mPosState->CreateInDocIterator());
        }
        return InDocPositionIteratorPtr();
    }

    void SetTermFreq(tf_t termFreq) { mPosState->SetTermFreq(termFreq);}
    
    void SetFirstOcc(pos_t firstOcc) 
    {
        mFirstOcc = firstOcc; 
        mIndexInfo.SetFirstOcc(true);
    }

    void SetFieldMap(fieldmap_t fieldMap) 
    {
        mFieldMap = fieldMap;
        mIndexInfo.SetFieldMap(true);
    }

    bool HasFieldMap() const { return mIndexInfo.HasFieldMap(); }
    
    bool HasFirstOcc() const { return mIndexInfo.HasFirstOcc(); }
    
    void SetDocPayload(docpayload_t docPayload) 
    {
        mDocPayload = docPayload;
        mIndexInfo.SetDocPayload(true);
    }

    void SetHasDocPayload(bool hasDocPayload) 
    { 
        mIndexInfo.SetDocPayload(hasDocPayload);
    }

    bool HasDocPayload() const { return mIndexInfo.HasDocPayload(); }

    bool IsMatched() const { return mIndexInfo.IsMatched(); }

    // for test
    void SetMatched(bool flag) { mIndexInfo.SetMatched(flag); }

private:
    InDocPositionState* mPosState;  // 8 bytes
    pos_t mFirstOcc;                // 4 bytes
    docpayload_t mDocPayload;       // 2 bytes
    fieldmap_t mFieldMap;           // 1 byte
    IndexInfo mIndexInfo;           // 1 byte
};

typedef std::tr1::shared_ptr<TermMatchData> TermMatchDataPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TERM_MATCH_DATA_H
