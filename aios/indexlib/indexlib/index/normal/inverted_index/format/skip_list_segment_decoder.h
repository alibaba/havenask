#ifndef __INDEXLIB_SKIP_LIST_SEGMENT_DECODER_H
#define __INDEXLIB_SKIP_LIST_SEGMENT_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/numeric_compress/reference_compress_int_encoder.h"
#include "indexlib/index/normal/inverted_index/format/buffered_segment_index_decoder.h"

IE_NAMESPACE_BEGIN(index);

template <class SkipListType>
class SkipListSegmentDecoder : public BufferedSegmentIndexDecoder
{
public:
    SkipListSegmentDecoder(autil::mem_pool::Pool *sessionPool,
                           common::ByteSliceReader *docListReader,
                           uint32_t docListBegin,
                           uint8_t docCompressMode);
    ~SkipListSegmentDecoder();

protected:
    void InitSkipList(uint32_t start, uint32_t end,
                      util::ByteSliceList *postingList, df_t df,
                      CompressMode compressMode);
    
    bool DecodeDocBuffer(docid_t startDocId, docid_t *docBuffer,
                         docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF);
    bool DecodeDocBufferMayCopy(docid_t startDocId, docid_t *&docBuffer,
                                docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF);

    bool DecodeCurrentTFBuffer(tf_t *tfBuffer);
    void DecodeCurrentDocPayloadBuffer(docpayload_t *docPayloadBuffer);
    void DecodeCurrentFieldMapBuffer(fieldmap_t *fieldBitmapBuffer);

private:
    SkipListType *mSkipListReader;
    autil::mem_pool::Pool *mSessionPool;
    common::ByteSliceReader *mDocListReader;
    uint32_t mDocListBeginPos;
    const common::Int32Encoder* mDocEncoder;
    const common::Int32Encoder* mTfEncoder;
    const common::Int16Encoder* mDocPayloadEncoder;
    const common::Int8Encoder* mFieldMapEncoder;

private:
    IE_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////////////////////////

template <class SkipListType>
SkipListSegmentDecoder<SkipListType>::SkipListSegmentDecoder(
        autil::mem_pool::Pool *sessionPool,
        common::ByteSliceReader *docListReader,
        uint32_t docListBegin,
        uint8_t docCompressMode)
    : mSkipListReader(NULL)
    , mSessionPool(sessionPool)
    , mDocListReader(docListReader)
    , mDocListBeginPos(docListBegin)
{
    mDocEncoder = common::EncoderProvider::GetInstance()->GetDocListEncoder(
            docCompressMode);
    mTfEncoder = common::EncoderProvider::GetInstance()->GetTfListEncoder(
            docCompressMode);
    mDocPayloadEncoder = common::EncoderProvider::GetInstance()->GetDocPayloadEncoder(
            docCompressMode);
    mFieldMapEncoder = common::EncoderProvider::GetInstance()->GetFieldMapEncoder(
            docCompressMode);
}

template <class SkipListType>
SkipListSegmentDecoder<SkipListType>::~SkipListSegmentDecoder() 
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSkipListReader);
}

template <class SkipListType>
void SkipListSegmentDecoder<SkipListType>::InitSkipList(uint32_t start, uint32_t end,
        util::ByteSliceList *postingList, df_t df, CompressMode compressMode)
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mSkipListReader);
    mSkipListReader = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            SkipListType, compressMode == REFERENCE_COMPRESS_MODE);
    mSkipListReader->Load(postingList, start, end,
                          (df - 1) / MAX_DOC_PER_RECORD + 1);
}

template <class SkipListType>
bool SkipListSegmentDecoder<SkipListType>::DecodeDocBuffer(
        docid_t startDocId, docid_t *docBuffer,
        docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    uint32_t offset;
    uint32_t recordLen;
    typename SkipListType::keytype_t lastDocIdInPrevRecord;

    if (!mSkipListReader->SkipTo((typename SkipListType::keytype_t)startDocId, 
                                 (typename SkipListType::keytype_t&)lastDocId,
                                 lastDocIdInPrevRecord,
                                 offset, recordLen))
    {
        return false;
    }
    currentTTF = mSkipListReader->GetPrevTTF();
    mSkipedItemCount = mSkipListReader->GetSkippedItemCount();

    mDocListReader->Seek(offset + this->mDocListBeginPos);
    mDocEncoder->Decode((uint32_t*)docBuffer, MAX_DOC_PER_RECORD, *mDocListReader);

    firstDocId = docBuffer[0] + lastDocIdInPrevRecord;
    return true;
}

template <class SkipListType>
bool SkipListSegmentDecoder<SkipListType>::DecodeDocBufferMayCopy(
        docid_t startDocId, docid_t *&docBuffer,
        docid_t &firstDocId, docid_t &lastDocId, ttf_t &currentTTF)
{
    uint32_t offset;
    uint32_t recordLen;
    typename SkipListType::keytype_t lastDocIdInPrevRecord;

    if (!mSkipListReader->SkipTo((typename SkipListType::keytype_t)startDocId, 
                                 (typename SkipListType::keytype_t&)lastDocId,
                                 lastDocIdInPrevRecord, offset, recordLen))
    {
        return false;
    }
    currentTTF = mSkipListReader->GetPrevTTF();
    mSkipedItemCount = mSkipListReader->GetSkippedItemCount();

    mDocListReader->Seek(offset + this->mDocListBeginPos);
    mDocEncoder->DecodeMayCopy((uint32_t*&)docBuffer, MAX_DOC_PER_RECORD, *mDocListReader);

    //TODO: merge with DecodeDocBuffer
    firstDocId = common::ReferenceCompressInt32Encoder::GetFirstValue((char*)docBuffer);
    return true;
}

template <class SkipListType>
bool SkipListSegmentDecoder<SkipListType>::DecodeCurrentTFBuffer(tf_t *tfBuffer)
{
    mTfEncoder->Decode((uint32_t*)tfBuffer, MAX_DOC_PER_RECORD, *mDocListReader);
    return true;
}

template <class SkipListType>
void SkipListSegmentDecoder<SkipListType>::DecodeCurrentDocPayloadBuffer(
        docpayload_t *docPayloadBuffer)
{
    mDocPayloadEncoder->Decode(docPayloadBuffer, MAX_DOC_PER_RECORD, 
                               *mDocListReader);    
}

template <class SkipListType>
void SkipListSegmentDecoder<SkipListType>::DecodeCurrentFieldMapBuffer(
        fieldmap_t *fieldBitmapBuffer)
{
    mFieldMapEncoder->Decode(fieldBitmapBuffer, MAX_DOC_PER_RECORD, *mDocListReader);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SKIP_LIST_SEGMENT_DECODER_H
