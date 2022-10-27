#ifndef __INDEXLIB_PACKAGE_TEXT_IN_DOC_POSITION_ITERATOR_H
#define __INDEXLIB_PACKAGE_TEXT_IN_DOC_POSITION_ITERATOR_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/accessor/in_doc_position_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_state.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format_option.h"
#include "indexlib/index/normal/attribute/multi_section_meta.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"

DECLARE_REFERENCE_CLASS(index, SinglePackPostingIterator);

IE_NAMESPACE_BEGIN(index);

class NormalInDocPositionIterator : public InDocPositionIterator
{
public:
    NormalInDocPositionIterator(index::PositionListFormatOption option =
                                index::PositionListFormatOption());
    ~NormalInDocPositionIterator();

public:
    void Init(const NormalInDocState& state);
    common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t &nextPos) override;
    pospayload_t GetPosPayload() override;
    sectionid_t GetSectionId() override ;
    section_len_t GetSectionLength() override ;
    section_weight_t GetSectionWeight() override ;
    fieldid_t GetFieldId() override ;
    int32_t GetFieldPosition() override ;
    bool HasPosPayload() const override;
    pos_t GetCurrentPosition() const { return mCurrentPos; }

private:
    inline void SeekSectionMeta();

private:
    pos_t mPosBuffer[MAX_POS_PER_RECORD];
    pospayload_t mPosPayloadBuffer[MAX_POS_PER_RECORD];
    
    NormalInDocState mState;

    int64_t mCurrentPos; // int64 for we can use -1 as initial stat
    int32_t mVisitedPosInBuffer;
    int32_t mVisitedPosInDoc;
    int32_t mPosCountInBuffer;
    uint32_t mOffsetInRecord;
    uint32_t mTotalPosCount;
    
    uint8_t mSectionMetaBuf[MAX_SECTION_COUNT_PER_DOC * 5] __attribute__ ((aligned (8)));
    MultiSectionMeta *mSectionMeta;
    int32_t mVisitedSectionInDoc;
    uint32_t mVisitedSectionLen;
    sectionid_t mCurrentSectionId;
    fieldid_t mCurrentFieldId;

    index::PositionListFormatOption mOption;

    friend class SinglePackPostingIterator;
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<NormalInDocPositionIterator> NormalInDocPositionIteratorPtr;
/////////////////////////////////////////////////////////////////////////////////////

inline void NormalInDocPositionIterator::SeekSectionMeta()
{
    if(mSectionMeta == NULL) {
        mSectionMeta = new MultiSectionMeta();
    }
    if (mSectionMeta->GetSectionCount() == 0)
    {
        const SectionAttributeReaderImpl* sectionReader = 
            static_cast<const SectionAttributeReaderImpl*>(mState.GetSectionReader());
        assert(sectionReader);
        if (0 > sectionReader->Read(mState.GetDocId(), 
                        mSectionMetaBuf, sizeof(mSectionMetaBuf)))
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read section attribute "
                    "FAILED, docId: [%d]", mState.GetDocId());
        }
    
        mSectionMeta->Init(mSectionMetaBuf, sectionReader->HasFieldId(), 
                          sectionReader->HasSectionWeight());
    }

    while (mCurrentPos >= mVisitedSectionLen)
    {
        mVisitedSectionLen += mSectionMeta->GetSectionLen(++mVisitedSectionInDoc);

        fieldid_t fieldId = mSectionMeta->GetFieldId(mVisitedSectionInDoc);
        if (mCurrentFieldId != fieldId)
        {
            mCurrentSectionId = 0;
            mCurrentFieldId = fieldId;
        }
        else 
        {
            mCurrentSectionId++;
        }
    }
}

inline bool NormalInDocPositionIterator::HasPosPayload() const
{
    return mOption.HasPositionPayload();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PACKAGE_TEXT_IN_DOC_POSITION_ITERATOR_H
