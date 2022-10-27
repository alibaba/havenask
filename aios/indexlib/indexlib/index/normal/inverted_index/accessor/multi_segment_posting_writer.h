#ifndef __INDEXLIB_MULTI_SEGMENT_POSTING_WRITER_H
#define __INDEXLIB_MULTI_SEGMENT_POSTING_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer_impl.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/index/normal/inverted_index/accessor/index_output_segment_resource.h"
#include "indexlib/common/in_mem_file_writer.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"

IE_NAMESPACE_BEGIN(index);

class MultiSegmentPostingWriter
{
public:
    MultiSegmentPostingWriter(index::PostingWriterResource* writerResource,
                              const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
                              PostingFormatOption& postingFormatOption,
                              bool disableDictInline = false)
    {
        assert(writerResource);
        mOutputSegmentMergeInfos = outputSegmentMergeInfos;
        for (size_t i = 0; i < outputSegmentMergeInfos.size(); i++)
        {
            PostingWriterPtr postingWriter(new PostingWriterImpl(writerResource, disableDictInline));
            mPostingWriters.push_back(postingWriter);
        }
        mPostingFormatOption = postingFormatOption;
    }
    
    ~MultiSegmentPostingWriter();

public:
    void EndSegment()
    {
        for (size_t i = 0; i < mPostingWriters.size(); i++)
        {
            mPostingWriters[i]->EndSegment();
        }
    }

    PostingWriterPtr GetSegmentPostingWriter(size_t segmentIdx)
    {
        if (segmentIdx >= mPostingWriters.size())
        {
            return PostingWriterPtr();
        }
        return mPostingWriters[segmentIdx];
    }
    
    void Dump(dictkey_t key, 
              IndexOutputSegmentResources& indexOutputSegmentResources,
              termpayload_t termPayLoad);
    df_t GetDF() const
    {
        df_t total = 0;
        for (auto postingWriter : mPostingWriters)
        {
            total += postingWriter->GetDF();            
        }
        return total;
    }
    ttf_t GetTotalTF() const
    {
        ttf_t total = 0;
        for (auto postingWriter : mPostingWriters)
        {
            total += postingWriter->GetTotalTF();            
        }
        return total;
    }
    uint64_t GetPostingLength() const
    {
        uint64_t total = 0;
        for (auto postingWriter : mPostingWriters)
        {
            total += postingWriter->GetDumpLength();            
        }
        return total;
    }

    PostingIteratorPtr CreatePostingIterator(termpayload_t termPayload);

private:
    PostingIteratorPtr CreateSinglePostingIterator(PostingWriterPtr& postingWriter,
            termpayload_t termPayload, common::InMemFileWriterPtr& fileWriterPtr);
    dictvalue_t GetDictValue(const PostingWriterPtr& postingWriter, uint64_t offset);

    void DumpPosting(dictkey_t key, 
                     IndexOutputSegmentResourcePtr& resource,
                     PostingWriterPtr& postingWriter,
                     termpayload_t termPayload);

    PostingIteratorPtr CreatePostingIterator(
            common::InMemFileWriterPtr& fileWriterPtr, 
            PostingWriterPtr& postingWriter,
            termpayload_t termPayload);
    uint64_t GetDumpLength(const PostingWriterPtr& postingWriter, termpayload_t termPayload) const;
    template <typename FileWriterPtr>
    void DoDumpPosting(
            PostingWriterPtr& postingWriter, FileWriterPtr& postingFile, termpayload_t termPayload);

public:
    std::vector<PostingWriterPtr> mPostingWriters;
    PostingFormatOption mPostingFormatOption;
    index_base::OutputSegmentMergeInfos mOutputSegmentMergeInfos;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiSegmentPostingWriter);
///////////////////////////////////////////////////////////////////
template <typename FileWriterPtr>
void MultiSegmentPostingWriter::DoDumpPosting(
        PostingWriterPtr& postingWriter, FileWriterPtr& postingFile, termpayload_t termPayload)
{
    index::TermMeta termMeta(postingWriter->GetDF(), postingWriter->GetTotalTF(), termPayload);
    TermMetaDumper tmDumper(mPostingFormatOption);
    
    uint32_t totalLen = tmDumper.CalculateStoreSize(termMeta) + postingWriter->GetDumpLength();
    if (mPostingFormatOption.IsCompressedPostingHeader())
    {
        postingFile->WriteVUInt32(totalLen);
    }
    else
    {
        postingFile->Write((const void*)&totalLen, sizeof(uint32_t));
    }
    tmDumper.Dump(postingFile, termMeta);
    postingWriter->Dump(postingFile);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_SEGMENT_POSTING_WRITER_H
