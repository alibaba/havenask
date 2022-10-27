#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/buffered_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/index_base/index_meta/segment_info.h"

using namespace std;
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiSegmentPostingWriter);
DECLARE_REFERENCE_CLASS(index, IndexOutputSegmentResource);


MultiSegmentPostingWriter::~MultiSegmentPostingWriter() 
{
}

void MultiSegmentPostingWriter::Dump(
        dictkey_t key,
        IndexOutputSegmentResources& indexOutputSegmentResources,
        termpayload_t termPayload)
{
    assert(indexOutputSegmentResources.size() == mOutputSegmentMergeInfos.size());
    for(size_t i = 0; i < indexOutputSegmentResources.size(); i++)
    {
        DumpPosting(key, indexOutputSegmentResources[i], mPostingWriters[i], termPayload);
    }
}

dictvalue_t MultiSegmentPostingWriter::GetDictValue(
        const PostingWriterPtr& postingWriter, uint64_t offset) 
{
    uint64_t inlinePostingValue;   
    if (postingWriter->GetDictInlinePostingValue(inlinePostingValue))
    {
        return ShortListOptimizeUtil::CreateDictInlineValue(inlinePostingValue);
    }
    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
            postingWriter->GetDF(),
            postingWriter->GetTotalTF(), 
            mPostingFormatOption.GetDocListCompressMode());
    return ShortListOptimizeUtil::CreateDictValue(compressMode, offset);
}

PostingIteratorPtr MultiSegmentPostingWriter::CreatePostingIterator(termpayload_t termPayload)
{
    vector<MultiSegmentPostingIterator::PostingIteratorInfo> postingIterators;
    vector<common::InMemFileWriterPtr> fileWriters;
    vector<segmentid_t> segmentIdxs;
    int64_t baseDocid = 0;
    for (int32_t i = 0; i < (int32_t)mPostingWriters.size(); i++)
    {
        auto postingWriter = mPostingWriters[i];
        if (postingWriter->GetDF() > 0)
        {
            common::InMemFileWriterPtr fileWriterPtr(new common::InMemFileWriter());
            PostingIteratorPtr postingIter =
                CreateSinglePostingIterator(postingWriter,
                                            termPayload, fileWriterPtr);
            postingIterators.push_back(make_pair(baseDocid, postingIter));
            fileWriters.push_back(fileWriterPtr);
            segmentIdxs.push_back(i);
        }
        baseDocid += mOutputSegmentMergeInfos[i].docCount;
    }

    MultiSegmentPostingIteratorPtr iter(new MultiSegmentPostingIterator());
    iter->Init(postingIterators, segmentIdxs, fileWriters);
    iter->Reset();
    return iter;
}

PostingIteratorPtr MultiSegmentPostingWriter::CreateSinglePostingIterator(
        PostingWriterPtr& postingWriter, termpayload_t termPayload, 
        common::InMemFileWriterPtr& fileWriterPtr)
{
    fileWriterPtr->Init(GetDumpLength(postingWriter, termPayload));
    PostingIteratorPtr postingIter = CreatePostingIterator(fileWriterPtr, postingWriter, termPayload);
    return postingIter;
}

PostingIteratorPtr MultiSegmentPostingWriter::CreatePostingIterator(
        common::InMemFileWriterPtr& fileWriterPtr,
        PostingWriterPtr& postingWriter, 
        termpayload_t termPayload)
{
    uint64_t dictInlineValue;
    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    if (postingWriter->GetDictInlinePostingValue(dictInlineValue))
    {
        SegmentPosting segPosting(mPostingFormatOption);
        segPosting.Init(
            0, SegmentInfo(), ShortListOptimizeUtil::CreateDictInlineValue(dictInlineValue));
        segPostings->push_back(std::move(segPosting));
    } 
    else
    {
        DoDumpPosting<common::InMemFileWriterPtr>(postingWriter, fileWriterPtr, termPayload);
        uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
            postingWriter->GetDF(), postingWriter->GetTotalTF(),
            mPostingFormatOption.GetDocListCompressMode());
        SegmentPosting segPosting(mPostingFormatOption);
        segPosting.Init(compressMode, fileWriterPtr->GetByteSliceList(true),
                        0, SegmentInfo());
        segPostings->push_back(std::move(segPosting));
    }

    BufferedPostingIterator* bufferedPostingIter = new BufferedPostingIterator(mPostingFormatOption, NULL);
    bufferedPostingIter->Init(segPostings, NULL, 1);
    PostingIteratorPtr postingIter(bufferedPostingIter);
    return postingIter;
}

uint64_t MultiSegmentPostingWriter::GetDumpLength(const PostingWriterPtr& postingWriter,
                                          termpayload_t termPayload) const
{

    index::TermMeta termMeta(
            postingWriter->GetDF(), postingWriter->GetTotalTF(), termPayload);
    TermMetaDumper tmDumper(mPostingFormatOption);
    uint64_t length
        = tmDumper.CalculateStoreSize(termMeta) + postingWriter->GetDumpLength();
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


void MultiSegmentPostingWriter::DumpPosting(dictkey_t key,
        IndexOutputSegmentResourcePtr& resource, PostingWriterPtr & postingWriter,
        termpayload_t termPayload)
{
    if (postingWriter->GetDF() <= 0)
    {
        return;
    }
    IndexDataWriterPtr termDataWriter = resource->GetIndexDataWriter(SegmentTermInfo::TM_NORMAL);
    auto postingFile = termDataWriter->postingWriter;
    dictvalue_t dictValue = GetDictValue(postingWriter, postingFile->GetLength());
    termDataWriter->dictWriter->AddItem(key, dictValue);
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(dictValue))
    {
        return;
    }

    // dump posting
    DoDumpPosting<file_system::FileWriterPtr>(postingWriter, postingFile, termPayload);
}

IE_NAMESPACE_END(index);
