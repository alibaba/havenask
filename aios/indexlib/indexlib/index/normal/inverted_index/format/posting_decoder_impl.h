#ifndef __INDEXLIB_POSTING_DECODER_IMPL_H
#define __INDEXLIB_POSTING_DECODER_IMPL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/util/bitmap.h"
#include "indexlib/index/normal/inverted_index/format/posting_decoder.h"
#include "indexlib/common/numeric_compress/new_pfordelta_int_encoder.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_decoder.h"

IE_NAMESPACE_BEGIN(index);

class PostingDecoderImpl : public index::PostingDecoder
{
public:
    PostingDecoderImpl(const PostingFormatOption &postingFormatOption);
    virtual ~PostingDecoderImpl();

public:
    void Init(index::TermMeta *termMeta, 
              const common::ByteSliceReaderPtr &postingListReader, 
              const common::ByteSliceReaderPtr &positionListReader,
              const util::BitmapPtr &tfBitmap);

    // init for dict inline compress
    void Init(index::TermMeta *termMeta, dictvalue_t dictValue);

    void SetCurrentTermHashKey(dictkey_t termHashKey)
    {  
        assert(termHashKey >= mCurrentTermHashKey); 
        mCurrentTermHashKey = termHashKey; 
    }

    dictkey_t GetCurrentTermHashKey() const 
    {  return mCurrentTermHashKey; }

    uint32_t DecodeDocList(docid_t *docIdBuf, tf_t *tfListBuf,
                           docpayload_t *docPayloadBuf, 
                           fieldmap_t *fieldMapBuf, 
                           size_t len);

    uint32_t DecodePosList(pos_t *posListBuf, 
                           pospayload_t *posPayloadBuf,
                           size_t len);

    bool IsDocEnd(uint32_t posIndex)
    {
        assert(mPostingFormatOption.HasTfBitmap());
        if (posIndex >= mTfBitmap->GetValidItemCount())
        {
            return true;
        }
        return mTfBitmap->Test(posIndex);
    }

private:
    // virtual for test
    virtual void InitDocListEncoder(
            const DocListFormatOption &docListFormatOption,
            df_t df);

    virtual void InitPosListEncoder(
            const PositionListFormatOption &posListFormatOption,
            ttf_t totalTF);

private:
    common::ByteSliceReaderPtr mPostingListReader;
    common::ByteSliceReaderPtr mPositionListReader;

    util::BitmapPtr mTfBitmap;
    dictvalue_t mInlineDictValue;

    const common::Int32Encoder* mDocIdEncoder;
    const common::Int32Encoder* mTFListEncoder;
    const common::Int16Encoder* mDocPayloadEncoder;
    const common::Int8Encoder*  mFieldMapEncoder;
    const common::Int32Encoder* mPositionEncoder;
    const common::Int8Encoder*  mPosPayloadEncoder;

    df_t mDecodedDocCount;
    tf_t mDecodedPosCount;

    dictkey_t mCurrentTermHashKey;
    PostingFormatOption mPostingFormatOption;

private:
    friend class PostingDecoderImplTest;
    friend class OneDocMergerTest;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingDecoderImpl);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_DECODER_IMPL_H
