
#include "indexlib/index/normal/inverted_index/format/position_list_encoder.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/in_mem_pair_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_position_list_decoder.h"

using namespace std;
using namespace std::tr1;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionListEncoder);

void PositionListEncoder::AddPosition(pos_t pos, pospayload_t posPayload)
{
    mPosListBuffer.PushBack(0, pos - mLastPosInCurDoc);

    if (mPosListFormatOption.HasPositionPayload())
    {
        mPosListBuffer.PushBack(1, posPayload);
    }

    mPosListBuffer.EndPushBack();

    mLastPosInCurDoc = pos;
    ++mTotalPosCount;

    if (mPosListBuffer.NeedFlush(MAX_POS_PER_RECORD))
    {
        FlushPositionBuffer(PFOR_DELTA_COMPRESS_MODE);
    }
}

void PositionListEncoder::EndDocument()
{
    mLastPosInCurDoc = 0;
}

void PositionListEncoder::Flush() 
{
    uint8_t compressMode = 
        ShortListOptimizeUtil::GetPosListCompressMode(mTotalPosCount);
    FlushPositionBuffer(compressMode);
    if (mPosSkipListWriter)
    {
        mPosSkipListWriter->FinishFlush();
    }
}

void PositionListEncoder::Dump(const file_system::FileWriterPtr& file)
{
    Flush();
    uint32_t posListSize = mPosListBuffer.EstimateDumpSize();
    uint32_t posSkipListSize = 0;
    if (mPosSkipListWriter)
    {
        posSkipListSize = mPosSkipListWriter->EstimateDumpSize();
    }

    file->WriteVUInt32(posSkipListSize);
    file->WriteVUInt32(posListSize);
    if (mPosSkipListWriter)
    {
        mPosSkipListWriter->Dump(file);            
    }
    mPosListBuffer.Dump(file);
    IE_LOG(TRACE1, "pos skip list length: [%u], pos list length: [%u",
           posSkipListSize, posListSize);
}

uint32_t PositionListEncoder::GetDumpLength() const
{
    uint32_t posSkipListSize = 0;
    if (mPosSkipListWriter)
    {
        posSkipListSize = mPosSkipListWriter->EstimateDumpSize();
    }

    uint32_t posListSize = mPosListBuffer.EstimateDumpSize();
    return VByteCompressor::GetVInt32Length(posSkipListSize)
        + VByteCompressor::GetVInt32Length(posListSize)
        + posSkipListSize
        + posListSize;
}

void PositionListEncoder::CreatePosSkipListWriter()
{
    assert(mPosSkipListWriter == NULL);
    RecyclePool* bufferPool = dynamic_cast<RecyclePool*>(
            mPosListBuffer.GetBufferPool());
    assert(bufferPool);
    
    void *buffer = mByteSlicePool->allocate(sizeof(BufferedSkipListWriter));
    BufferedSkipListWriter* posSkipListWriter = new(buffer) BufferedSkipListWriter(
            mByteSlicePool, bufferPool);
    posSkipListWriter->Init(mPosListFormat->GetPositionSkipListFormat());

    // skip list writer should be atomic created;
    mPosSkipListWriter = posSkipListWriter;
}

void PositionListEncoder::AddPosSkipListItem(
        uint32_t totalPosCount, uint32_t compressedPosSize, bool needFlush)
{
    if (mPosListFormatOption.HasTfBitmap())
    {
        if (needFlush)
        {
            mPosSkipListWriter->AddItem(compressedPosSize);
        }
    }
    else
    {
        mPosSkipListWriter->AddItem(totalPosCount, compressedPosSize);
    }
}

void PositionListEncoder::FlushPositionBuffer(uint8_t compressMode)
{
    //TODO: uncompress need this
    bool needFlush = mPosListBuffer.NeedFlush(MAX_POS_PER_RECORD);

    uint32_t flushSize = mPosListBuffer.Flush(compressMode);
    if (flushSize > 0)
    {
        if (compressMode != SHORT_LIST_COMPRESS_MODE)
        {
            if (mPosSkipListWriter == NULL)
            {
                CreatePosSkipListWriter();
            }
            AddPosSkipListItem(mTotalPosCount, flushSize, needFlush);
        }
    }
}

InMemPositionListDecoder* PositionListEncoder::GetInMemPositionListDecoder(
        Pool *sessionPool) const
{
    // doclist -> ttf -> pos skiplist -> poslist
    ttf_t ttf = mTotalPosCount;

    //TODO: delete buffer in pool
    InMemPairValueSkipListReader* inMemSkipListReader = NULL;
    if (mPosSkipListWriter) 
    {
        // not support tf bitmap in realtime segment
        assert(!mPosListFormatOption.HasTfBitmap());
        inMemSkipListReader = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
                InMemPairValueSkipListReader, sessionPool, false);
        inMemSkipListReader->Load(mPosSkipListWriter);
    }
    BufferedByteSlice* posListBuffer = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            BufferedByteSlice, sessionPool, sessionPool);
    mPosListBuffer.SnapShot(posListBuffer);

    InMemPositionListDecoder* decoder = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            InMemPositionListDecoder, mPosListFormatOption, sessionPool);
    decoder->Init(ttf, inMemSkipListReader, posListBuffer);

    return decoder;
}

IE_NAMESPACE_END(index);

