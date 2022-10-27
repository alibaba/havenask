#ifndef __INDEXLIB_POSITION_LIST_SEGMENT_DECODER_H
#define __INDEXLIB_POSITION_LIST_SEGMENT_DECODER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_in_doc_state.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/pair_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/position_bitmap_reader.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format_option.h"

IE_NAMESPACE_BEGIN(index);

class PositionListSegmentDecoder
{
public:
    PositionListSegmentDecoder(const PositionListFormatOption &option,
                               autil::mem_pool::Pool* sessionPool);
    virtual ~PositionListSegmentDecoder();

public:
    void Init(const util::ByteSliceList* posList, tf_t totalTF,
              uint32_t posListBegin, uint8_t compressMode,
              index::NormalInDocState* state);

    virtual bool SkipTo(ttf_t currentTTF, index::NormalInDocState* state);

    void SetNeedReopen(bool needReopen) 
    {
        mNeedReopen = needReopen;
    }
    
    // TODO: separate TfBitmap CalculateRecordOffset from LocateRecord
    virtual bool LocateRecord(const index::NormalInDocState* state, uint32_t& termFrequence);

    virtual uint32_t DecodeRecord(pos_t* posBuffer, uint32_t posBufferLen,
                    pospayload_t* payloadBuffer, uint32_t payloadBufferLen);

    uint32_t GetRecordOffset() const 
    { return mRecordOffset; }

    int32_t GetOffsetInRecord() const
    { return mOffsetInRecord; }

    index::PositionBitmapReader* GetPositionBitmapReader() const
    { return mBitmapReader; }

private:
    void InitPositionBitmapReader(
            const util::ByteSliceList* posList,
            uint32_t& posListCursor,
            index::NormalInDocState* state);

    void InitPositionBitmapBlockBuffer(
            common::ByteSliceReader& posListReader,
            tf_t totalTF,
            uint32_t posSkipListStart,
            uint32_t posSkipListLen,
            index::NormalInDocState* state);

    void InitPositionSkipList(
            const util::ByteSliceList* posList,
            tf_t totalTF,
            uint32_t posSkipListStart,
            uint32_t posSkipListLen,
            index::NormalInDocState* state);

    void CalculateRecordOffsetByPosBitmap(
            const index::NormalInDocState *state,
            uint32_t& termFrequence,
            uint32_t& recordOffset,
            int32_t& offsetInRecord);

protected:
    index::PairValueSkipListReader* mPosSkipListReader;
    autil::mem_pool::Pool* mSessionPool;
    const common::Int32Encoder* mPosEncoder;
    const common::Int8Encoder* mPosPayloadEncoder;

    index::PositionBitmapReader* mBitmapReader;
    uint32_t *mPosBitmapBlockBuffer;
    uint32_t mPosBitmapBlockCount;
    uint32_t mTotalTf;
    
    uint32_t mDecodedPosCount;
    uint32_t mRecordOffset;
    uint32_t mPreRecordTTF;
    int32_t mOffsetInRecord;

    uint32_t mPosListBegin;
    uint32_t mLastDecodeOffset;
    PositionListFormatOption mOption;
    bool mOwnPosBitmapBlockBuffer;
    bool mNeedReopen;

private:
    common::ByteSliceReader mPosListReader;

private:
    friend class PositionListSegmentDecoderTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PositionListSegmentDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITION_LIST_SEGMENT_DECODER_H
