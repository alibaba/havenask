#include "indexlib/index/normal/inverted_index/accessor/position_bitmap_writer.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionBitmapWriter);

void PositionBitmapWriter::Set(uint32_t index)
{
    mBitmap.Set(index);
}

void PositionBitmapWriter::Resize(uint32_t size)
{
    mBitmap.ReSize(size);
}

void PositionBitmapWriter::EndDocument(uint32_t df,
                                       uint32_t totalPosCount)
{
    if (df % MAX_DOC_PER_BITMAP_BLOCK == 0)
    {
        mBlockOffsets.push_back(totalPosCount - 1);
    }
}

uint32_t PositionBitmapWriter::GetDumpLength(uint32_t bitCount) const
{
    return VByteCompressor::GetVInt32Length(mBlockOffsets.size())
        + VByteCompressor::GetVInt32Length(bitCount)
        + mBlockOffsets.size() * sizeof(uint32_t)
        + Bitmap::GetDumpSize(bitCount);
}

void PositionBitmapWriter::Dump(FileWriter* file,
                                uint32_t bitCount)
{
    file->WriteVUInt32(mBlockOffsets.size());
    file->WriteVUInt32(bitCount);
    for (uint32_t i = 0; i < mBlockOffsets.size(); ++i)
    {
        file->Write((void*)(&mBlockOffsets[i]), sizeof(uint32_t));
    }
    file->Write((void*)mBitmap.GetData(), Bitmap::GetDumpSize(bitCount));
}

void PositionBitmapWriter::Dump(const file_system::FileWriterPtr& file,
                                uint32_t bitCount)
{
    file->WriteVUInt32(mBlockOffsets.size());
    file->WriteVUInt32(bitCount);
    for (uint32_t i = 0; i < mBlockOffsets.size(); ++i)
    {
        file->Write((void*)(&mBlockOffsets[i]), sizeof(uint32_t));
    }
    file->Write((void*)mBitmap.GetData(), Bitmap::GetDumpSize(bitCount));
}

IE_NAMESPACE_END(index);

