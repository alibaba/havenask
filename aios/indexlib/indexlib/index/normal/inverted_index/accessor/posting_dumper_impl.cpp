#include "indexlib/index/normal/inverted_index/accessor/posting_dumper_impl.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingDumperImpl);

PostingDumperImpl::PostingDumperImpl() { }

PostingDumperImpl::~PostingDumperImpl() {  }

void PostingDumperImpl::Dump(const file_system::FileWriterPtr& postingFile)
{
    index::TermMeta termMeta(mPostingWriter->GetDF(),
                              mPostingWriter->GetTotalTF(), mTermPayload);
    TermMetaDumper tmDumper(mPostingFormatOption);

    uint32_t totalLen = tmDumper.CalculateStoreSize(termMeta) +
                        mPostingWriter->GetDumpLength();
    if (mPostingFormatOption.IsCompressedPostingHeader())
    {
        postingFile->WriteVUInt32(totalLen);
    }
    else
    {
        postingFile->Write((const void*)&totalLen, sizeof(uint32_t));
    }
    tmDumper.Dump(postingFile, termMeta);
    mPostingWriter->Dump(postingFile);
}

uint64_t PostingDumperImpl::GetDumpLength() const
{
    index::TermMeta termMeta(mPostingWriter->GetDF(),
                              mPostingWriter->GetTotalTF(), mTermPayload);
    TermMetaDumper tmDumper(mPostingFormatOption);

    uint64_t length = tmDumper.CalculateStoreSize(termMeta) + mPostingWriter->GetDumpLength();
    if (mPostingFormatOption.IsCompressedPostingHeader())
    {
        length += common::VByteCompressor::GetVInt32Length(length);
    }
    else
    {
        length += sizeof(uint32_t);
    }
    return length;
}




IE_NAMESPACE_END(index);

