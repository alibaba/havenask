#ifndef __INDEXLIB_POSITION_LIST_ENCODER_H
#define __INDEXLIB_POSITION_LIST_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format_option.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/buffered_skip_list_writer.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_position_list_decoder.h"

IE_NAMESPACE_BEGIN(index);

class PositionListEncoder
{
public:
    PositionListEncoder(const PositionListFormatOption& posListFormatOption,
                        autil::mem_pool::Pool* byteSlicePool,
                        autil::mem_pool::RecyclePool* bufferPool,
                        const PositionListFormat* posListFormat = NULL)
        : mPosListBuffer(byteSlicePool, bufferPool)
        , mLastPosInCurDoc(0)
        , mTotalPosCount(0)
        , mPosListFormatOption(posListFormatOption)
        , mIsOwnFormat(false)
        , mPosSkipListWriter(NULL)
        , mPosListFormat(posListFormat)
        , mByteSlicePool(byteSlicePool)
    {
        assert(posListFormatOption.HasPositionList());
        if (!posListFormat)
        {
            mPosListFormat = new PositionListFormat(posListFormatOption);
            mIsOwnFormat = true;
        }
        mPosListBuffer.Init(mPosListFormat);
    }

    ~PositionListEncoder()
    {
        if (mPosSkipListWriter)
        {
            mPosSkipListWriter->~BufferedSkipListWriter();
            mPosSkipListWriter = NULL;
        }
        if (mIsOwnFormat)
        {
            DELETE_AND_SET_NULL(mPosListFormat);
        }
    }

public:
    void AddPosition(pos_t pos, pospayload_t posPayload);
    void EndDocument();
    void Flush();
    void Dump(const file_system::FileWriterPtr& file);
    uint32_t GetDumpLength() const;

    InMemPositionListDecoder* GetInMemPositionListDecoder(
            autil::mem_pool::Pool *sessionPool) const;

private:
    void CreatePosSkipListWriter();
    void AddPosSkipListItem(uint32_t totalPosCount, uint32_t compressedPosSize,
                            bool needFlush);
    void FlushPositionBuffer(uint8_t compressMode);

public:
    //public for test
    const util::ByteSliceList* GetPositionList() const
    { return mPosListBuffer.GetByteSliceList(); }

    //only for ut
    void SetDocSkipListWriter(index::BufferedSkipListWriter* writer)
    { mPosSkipListWriter = writer; }

    const PositionListFormat* GetPositionListFormat() const
    { return mPosListFormat; }

private:
    BufferedByteSlice mPosListBuffer;
    pos_t    mLastPosInCurDoc; // 4byte
    uint32_t mTotalPosCount;   // 4byte
    PositionListFormatOption mPosListFormatOption; // 1byte
    bool mIsOwnFormat;         // 1byte
    index::BufferedSkipListWriter* mPosSkipListWriter;
    const PositionListFormat* mPosListFormat;
    autil::mem_pool::Pool* mByteSlicePool;

private:
    friend class PositionListEncoderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PositionListEncoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSITION_LIST_ENCODER_H
