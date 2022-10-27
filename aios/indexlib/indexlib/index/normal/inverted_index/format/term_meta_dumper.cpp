#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"

using namespace std;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);

TermMetaDumper::TermMetaDumper(const index::PostingFormatOption &option)
    : mOption(option)
{}

uint32_t TermMetaDumper::CalculateStoreSize(const TermMeta& termMeta) const
{
    uint32_t len =  VByteCompressor::GetVInt32Length(termMeta.GetDocFreq());
    bool isCompressed = mOption.IsCompressedPostingHeader();
    if (!isCompressed || mOption.HasTermFrequency())
    {
        len += VByteCompressor::GetVInt32Length(termMeta.GetTotalTermFreq());
    }
    if (!isCompressed || mOption.HasTermPayload())
    {
        len += sizeof(termpayload_t);
    }
    return len;
}

void TermMetaDumper::Dump(const file_system::FileWriterPtr& file,
                          const TermMeta& termMeta) const
{
    assert(file != NULL);

    file->WriteVUInt32(termMeta.GetDocFreq());
    bool isCompressed = mOption.IsCompressedPostingHeader();
    if (!isCompressed || mOption.HasTermFrequency())
    {
        file->WriteVUInt32(termMeta.GetTotalTermFreq());
    }
    if (!isCompressed || mOption.HasTermPayload())
    {
        termpayload_t payload = termMeta.GetPayload();
        file->Write((void*)(&payload), sizeof(payload));
    }
}

IE_NAMESPACE_END(index);

