#include "indexlib/index/normal/inverted_index/format/posting_decoder_impl.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingDecoderImpl);

PostingDecoderImpl::PostingDecoderImpl(
        const PostingFormatOption &postingFormatOption)
    : mInlineDictValue(INVALID_DICT_VALUE)
    , mDocIdEncoder(NULL)
    , mTFListEncoder(NULL)
    , mDocPayloadEncoder(NULL)
    , mFieldMapEncoder(NULL)
    , mPositionEncoder(NULL)
    , mPosPayloadEncoder(NULL)
    , mDecodedDocCount(0)
    , mDecodedPosCount(0)
    , mCurrentTermHashKey(0)
    , mPostingFormatOption(postingFormatOption)
{
}

PostingDecoderImpl::~PostingDecoderImpl() 
{
}

void PostingDecoderImpl::Init(
        TermMeta *termMeta, 
        const ByteSliceReaderPtr &postingListReader, 
        const ByteSliceReaderPtr &positionListReader,
        const BitmapPtr &tfBitmap)
{
    mTermMeta = termMeta;
    mPostingListReader = postingListReader;
    mPositionListReader = positionListReader;
    mTfBitmap = tfBitmap;
    mInlineDictValue = INVALID_DICT_VALUE;
    mDecodedDocCount = 0;
    mDecodedPosCount = 0;

    mDocIdEncoder = NULL;
    mTFListEncoder = NULL;
    mDocPayloadEncoder = NULL;
    mFieldMapEncoder = NULL;
    mPositionEncoder = NULL;
    mPosPayloadEncoder = NULL;

    InitDocListEncoder(
            mPostingFormatOption.GetDocListFormatOption(), 
            mTermMeta->GetDocFreq());

    InitPosListEncoder(
            mPostingFormatOption.GetPosListFormatOption(), 
            mTermMeta->GetTotalTermFreq());

}

void PostingDecoderImpl::Init(
        TermMeta *termMeta, dictvalue_t dictValue)
{
    mTermMeta = termMeta;
    mPostingListReader.reset();
    mPositionListReader.reset();
    mTfBitmap.reset();
    mInlineDictValue = dictValue;
    mDecodedDocCount = 0;
    mDecodedPosCount = 0;

    mDocIdEncoder = NULL;
    mTFListEncoder = NULL;
    mDocPayloadEncoder = NULL;
    mFieldMapEncoder = NULL;
    mPositionEncoder = NULL;
    mPosPayloadEncoder = NULL;

    DictInlineFormatter formatter(mPostingFormatOption, dictValue);
 
    mTermMeta->SetDocFreq(formatter.GetDocFreq());    
    mTermMeta->SetPayload(formatter.GetTermPayload());
    mTermMeta->SetTotalTermFreq(formatter.GetTermFreq());
}

uint32_t PostingDecoderImpl::DecodeDocList(
        docid_t* docIdBuf, tf_t* tfListBuf,
        docpayload_t* docPayloadBuf, 
        fieldmap_t* fieldMapBuf, 
        size_t len)
{
    if (mDecodedDocCount >= mTermMeta->GetDocFreq())
    {
        return 0;
    }

    // decode dict inline doclist
    if (mInlineDictValue != INVALID_DICT_VALUE)
    {
        DictInlineFormatter formatter(mPostingFormatOption, mInlineDictValue);        
        docIdBuf[0] = formatter.GetDocId();
        docPayloadBuf[0] = formatter.GetDocPayload();
        tfListBuf[0] = formatter.GetTermFreq();
        fieldMapBuf[0] = formatter.GetFieldMap();

        mDecodedDocCount++;
        return 1;
    }

    // decode normal doclist
    uint32_t docLen = mDocIdEncoder->Decode((uint32_t*)docIdBuf, len,
            *mPostingListReader);
    if (mTFListEncoder)
    {
        uint32_t tfLen = mTFListEncoder->Decode((uint32_t*)tfListBuf, len,
                *mPostingListReader);
        if (docLen != tfLen)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "doc/tf-list collapsed: [%u]/[%u]", docLen, tfLen);
        }
    }

    if (mDocPayloadEncoder)
    {
        uint32_t payloadLen = mDocPayloadEncoder->Decode(docPayloadBuf,
                len, *mPostingListReader);
        if (payloadLen != docLen)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "doc/docpayload-list collapsed: [%u]/[%u]",
                    docLen, payloadLen);
        }
    }

    if (mFieldMapEncoder)
    {
        uint32_t fieldmapLen = mFieldMapEncoder->Decode(fieldMapBuf, len, *mPostingListReader);
        if (fieldmapLen != docLen)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "doc/fieldmap-list collapsed: [%u]/[%u]",
                    docLen, fieldmapLen);
        }
    }
    mDecodedDocCount += docLen;
    return docLen;
}

uint32_t PostingDecoderImpl::DecodePosList(
        pos_t* posListBuf, 
        pospayload_t* posPayloadBuf,
        size_t len)
{
    if (mInlineDictValue != INVALID_DICT_VALUE)
    {
        return 0;
    }

    if (mDecodedPosCount >= mTermMeta->GetTotalTermFreq())
    {
        return 0;
    }
    uint32_t decodePosCount = 0;
    if (mPositionEncoder)
    {
        decodePosCount = mPositionEncoder->Decode(posListBuf, len,
                *mPositionListReader);
    }

    if (mPosPayloadEncoder)
    {
        uint32_t payloadLen = mPosPayloadEncoder->Decode(
                posPayloadBuf, len, *mPositionListReader);

        if (payloadLen != decodePosCount)
        {
            INDEXLIB_FATAL_ERROR(IndexCollapsed,
                    "pos/pospayload-list collapsed: [%u]/[%u]",
                    decodePosCount, payloadLen);
        }
    }
    mDecodedPosCount += decodePosCount;
    return decodePosCount;
}

void PostingDecoderImpl::InitDocListEncoder(
        const DocListFormatOption &docListFormatOption,
        df_t df)
{
    uint8_t docCompressMode = ShortListOptimizeUtil::GetDocListCompressMode(
            df, mPostingFormatOption.GetDocListCompressMode());
    mDocIdEncoder = EncoderProvider::GetInstance()->GetDocListEncoder(
            docCompressMode);
    if (docListFormatOption.HasTfList())
    {
        mTFListEncoder = EncoderProvider::GetInstance()->GetTfListEncoder(
                docCompressMode);
    }

    if (docListFormatOption.HasDocPayload())
    {
        mDocPayloadEncoder = EncoderProvider::GetInstance()->GetDocPayloadEncoder(
                docCompressMode);
    }

    if (docListFormatOption.HasFieldMap())
    {
        mFieldMapEncoder = EncoderProvider::GetInstance()->GetFieldMapEncoder(
                docCompressMode);
    }

    if (docListFormatOption.HasTfBitmap() 
        && mTfBitmap.get() == NULL)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "PositionBitmap is Null when HasTfBitmap!");
    }
}

void PostingDecoderImpl::InitPosListEncoder(
        const PositionListFormatOption &posListFormatOption,
        ttf_t totalTF)
{
    if (!posListFormatOption.HasPositionList())
    {
        return;
    }

    uint8_t posCompressMode = 
        ShortListOptimizeUtil::GetPosListCompressMode(totalTF);
    mPositionEncoder = EncoderProvider::GetInstance()->GetPosListEncoder(
            posCompressMode);
    if (posListFormatOption.HasPositionPayload())
    {
        mPosPayloadEncoder = EncoderProvider::GetInstance()
                             ->GetPosPayloadEncoder(posCompressMode);
    }
}

IE_NAMESPACE_END(index);

