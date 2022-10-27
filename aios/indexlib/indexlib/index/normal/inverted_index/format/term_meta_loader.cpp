#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/common/byte_slice_reader.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

TermMetaLoader::TermMetaLoader(const index::PostingFormatOption &option)
    : mOption(option)
{}
void TermMetaLoader::Load(ByteSliceReader* sliceReader, TermMeta& termMeta) const
{
    df_t df = (df_t)sliceReader->ReadVUInt32();
    termMeta.SetDocFreq(df);
    bool isCompressed = mOption.IsCompressedPostingHeader();
    if (!isCompressed || mOption.HasTermFrequency())
    {
        termMeta.SetTotalTermFreq((tf_t)sliceReader->ReadVUInt32());
    }
    else
    {
        termMeta.SetTotalTermFreq(df);
    }
    if (!isCompressed || mOption.HasTermPayload())
    {
        termpayload_t payload;
        sliceReader->Read((void*)(&payload), sizeof(payload));
        termMeta.SetPayload(payload);
    }
    else
    {
        termMeta.SetPayload(0);
    }
}

void TermMetaLoader::Load(const file_system::FileReaderPtr& reader,
                          TermMeta& termMeta) const
{
    df_t df = (df_t)reader->ReadVUInt32();
    termMeta.SetDocFreq(df);
    bool isCompressed = mOption.IsCompressedPostingHeader();
    if (!isCompressed || mOption.HasTermFrequency())
    {
        termMeta.SetTotalTermFreq((tf_t)reader->ReadVUInt32());
    }
    else
    {
        termMeta.SetTotalTermFreq(df);
    }
    if (!isCompressed || mOption.HasTermPayload())
    {
        termpayload_t payload;
        reader->Read((void*)(&payload), sizeof(payload));
        termMeta.SetPayload(payload);
    }
    else
    {
        termMeta.SetPayload(0);
    }
}

void TermMetaLoader::Load(uint8_t*& dataCursor, size_t& leftSize,
                          TermMeta& termMeta) const
{
    df_t df = VByteCompressor::DecodeVInt32(dataCursor, (uint32_t&)leftSize);
    termMeta.SetDocFreq(df);
    bool isCompressed = mOption.IsCompressedPostingHeader();
    if (!isCompressed || mOption.HasTermFrequency())
    {
        tf_t ttf = VByteCompressor::DecodeVInt32(dataCursor, (uint32_t&)leftSize);
        termMeta.SetTotalTermFreq(ttf);
    }
    else
    {
        termMeta.SetTotalTermFreq(df);
    }
    if (!isCompressed || mOption.HasTermPayload())
    {
        termpayload_t payload = *(termpayload_t*)dataCursor;
        dataCursor += sizeof(termpayload_t);
        leftSize -=  sizeof(termpayload_t);

        termMeta.SetPayload(payload);
    }
    else
    {
        termMeta.SetPayload(0);
    }
}

IE_NAMESPACE_END(index);

