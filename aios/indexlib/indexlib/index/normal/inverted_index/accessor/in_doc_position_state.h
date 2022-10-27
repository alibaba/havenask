#ifndef __INDEXLIB_IN_DOC_POSITION_STATE_H
#define __INDEXLIB_IN_DOC_POSITION_STATE_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/section_attribute_reader.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format_option.h"

IE_NAMESPACE_BEGIN(index);

class InDocPositionState
{
public:
    InDocPositionState(const index::PositionListFormatOption& option = 
                       index::PositionListFormatOption())
        : mTermFreq(0)
        , mDocId(INVALID_DOCID)
        , mSeekedDocCount(0)
        , mHasSectionReader(false)
        , mOption(option)
    {
        mData.sectionReader = NULL;
    }

    InDocPositionState(const InDocPositionState& other)
        : mTermFreq(other.mTermFreq)
        , mDocId(other.mDocId)
        , mSeekedDocCount(other.mSeekedDocCount)
        , mHasSectionReader(other.mHasSectionReader)
        , mOption(other.mOption)
    {
        mData = other.mData;
    }

    virtual ~InDocPositionState() {}

public:
    virtual InDocPositionIterator* CreateInDocIterator() const = 0;
    
    virtual void Free() = 0;
    
    virtual tf_t GetTermFreq()
    {
        if (mTermFreq == 0)
        {
            mTermFreq = 1;
        }
        return mTermFreq;
    }

    void SetDocId(docid_t docId)
    {
        mDocId = docId;
    }

    docid_t GetDocId() const
    {
        return mDocId;
    }

    void SetTermFreq(tf_t termFreq)
    {
        mTermFreq = termFreq;
    }

    void SetSeekedDocCount(uint32_t seekedDocCount)
    {
        mSeekedDocCount = seekedDocCount;
    }

    uint32_t GetSeekedDocCount() const
    {
        return mSeekedDocCount;
    }

    fieldid_t GetFieldId(int32_t fieldIdxInPack) const
    {
        return mData.sectionReader->GetFieldId(fieldIdxInPack);
    }

    void SetSectionReader(const SectionAttributeReader* sectionReader)
    {
        if (sectionReader)
        {
            mHasSectionReader = true;
            mData.sectionReader = sectionReader;
        }
    } 

    const SectionAttributeReader* GetSectionReader() const
    {
        if (!mHasSectionReader)
        {
            return NULL;
        }
        return mData.sectionReader;
    }

protected:
    void Clone(InDocPositionState* state) const
    {
        state->mData = mData;
        state->mTermFreq = mTermFreq;
        state->mDocId = mDocId;
        state->mSeekedDocCount = mSeekedDocCount;
        state->mHasSectionReader = mHasSectionReader;
        state->mOption = mOption;
    }

protected:
    union StateData
    {
        // allways be const SectionAttributeReaderImpl*
        const SectionAttributeReader* sectionReader;
    };

    StateData mData;                                   // 8 byte
    tf_t mTermFreq;                                    // 4 byte
    docid_t mDocId;                                    // 4 byte
    uint32_t mSeekedDocCount;                          // 4 byte
    bool mHasSectionReader;                            // 1 byte
    index::PositionListFormatOption mOption;  // 1 byte
};

typedef std::tr1::shared_ptr<InDocPositionState> InDocPositionStatePtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_DOC_POSITION_STATE_H
