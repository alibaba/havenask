#ifndef __INDEXLIB_MULTI_SEGMENT_BITMAP_POSTING_WRITER_H
#define __INDEXLIB_MULTI_SEGMENT_BITMAP_POSTING_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <autil/mem_pool/PoolBase.h>
#include "indexlib/index/normal/inverted_index/accessor/index_output_segment_resource.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/common/in_mem_file_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_iterator.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"

IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

class MultiSegmentBitmapPostingWriter
{
public:
    MultiSegmentBitmapPostingWriter(autil::mem_pool::PoolBase* pool,
                                    const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
    {
        for (size_t i = 0; i < outputSegmentMergeInfos.size(); i++)
        {
            BitmapPostingWriterPtr writer(new BitmapPostingWriter(pool));
            mPostingWriters.push_back(writer);
        }
        mOutputSegmentMergeInfos = outputSegmentMergeInfos;
    }
    ~MultiSegmentBitmapPostingWriter();

public:
    void EndSegment()
    {
        for (auto writer : mPostingWriters)
        {
            writer->EndSegment();            
        }
    }

    PostingWriterPtr GetSegmentBitmapPostingWriter(size_t segmentIdx)
    {
        if (segmentIdx >= mPostingWriters.size())
        {
            return PostingWriterPtr();
        }
        return mPostingWriters[segmentIdx];
    }
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

    void Dump(dictkey_t key, 
              IndexOutputSegmentResources& indexOutputSegmentResources, 
              termpayload_t termPayLoad);

    PostingIteratorPtr CreatePostingIterator(optionflag_t optionFlag, termpayload_t termPayload);
    uint64_t GetDumpLength() const;

private:
    void DumpPosting(dictkey_t key, IndexOutputSegmentResourcePtr& resource,
        PostingWriterPtr postingWriter, termpayload_t termPayload);
    template<typename FileWriterPtr>
    void DoDumpPosting(PostingWriterPtr postingWriter, FileWriterPtr& fileWriterPtr,
        termpayload_t termPayload);

    PostingIteratorPtr CreateSinglePostingIterator(InMemFileWriterPtr& fileWriterPtr,
        BitmapPostingWriterPtr& postingWriter, optionflag_t optionFlag, termpayload_t termPayLoad);
    uint64_t GetDumpLength(BitmapPostingWriterPtr& postingWriter) const;
 
public:
    std::vector<BitmapPostingWriterPtr> mPostingWriters;
    PostingFormatOption mPostingFormatOption;
    index_base::OutputSegmentMergeInfos mOutputSegmentMergeInfos;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiSegmentBitmapPostingWriter);
///////////////////////////////////////////////////////////
template<typename FileWriterPtr>
void MultiSegmentBitmapPostingWriter::DoDumpPosting(
        PostingWriterPtr postingWriter, 
        FileWriterPtr& fileWriterPtr,
        termpayload_t termPayload)
{
    // IndexDataWriterPtr termDataWriter = resource.GetIndexDataWriter(SegmentTermInfo::TM_BITMAP);
    // auto postingFile = termDataWriter->postingWriter;
    postingWriter->SetTermPayload(termPayload);
    uint32_t totalLength = postingWriter->GetDumpLength();
    fileWriterPtr->Write((void*)&totalLength, sizeof(uint32_t));
    postingWriter->Dump(fileWriterPtr);
}



IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_SEGMENT_BITMAP_POSTING_WRITER_H
