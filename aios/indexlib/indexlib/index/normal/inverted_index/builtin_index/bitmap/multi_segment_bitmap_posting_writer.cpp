#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/multi_segment_bitmap_posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"
#include "indexlib/index_base/index_meta/segment_info.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiSegmentBitmapPostingWriter);

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);

MultiSegmentBitmapPostingWriter::~MultiSegmentBitmapPostingWriter() 
{
}

void MultiSegmentBitmapPostingWriter::Dump(dictkey_t key,
        IndexOutputSegmentResources& indexOutputSegmentResources,
        termpayload_t termPayLoad)
{
    assert(indexOutputSegmentResources.size() == mOutputSegmentMergeInfos.size());
    for(size_t i = 0; i < indexOutputSegmentResources.size(); i++)
    {
        DumpPosting(key, indexOutputSegmentResources[i], mPostingWriters[i], termPayLoad);
    }
}

void MultiSegmentBitmapPostingWriter::DumpPosting(dictkey_t key,
        IndexOutputSegmentResourcePtr& resource, 
        PostingWriterPtr postingWriter,
        termpayload_t termPayload)
{
    if (postingWriter->GetDF() <= 0)
    {
        return;
    }

    IndexDataWriterPtr termDataWriter = resource->GetIndexDataWriter(SegmentTermInfo::TM_BITMAP);
    auto postingFile = termDataWriter->postingWriter;
    dictvalue_t dictValue = postingFile->GetLength();
    termDataWriter->dictWriter->AddItem(key, dictValue);
    postingWriter->SetTermPayload(termPayload);
    uint32_t totalLength =
        postingWriter->GetDumpLength();
    postingFile->Write((void*)&totalLength, sizeof(uint32_t));
    postingWriter->Dump(postingFile);
}

PostingIteratorPtr MultiSegmentBitmapPostingWriter::CreatePostingIterator(optionflag_t optionFlag, termpayload_t termPayload) 
{
    vector<MultiSegmentPostingIterator::PostingIteratorInfo> postingIterators;
    vector<InMemFileWriterPtr> fileWriters;
    vector<segmentid_t> segmentIdxs;
    int64_t baseDocid = 0;
    for (int32_t i = 0; i < (int32_t)mPostingWriters.size(); i++)
    {
        auto postingWriter = mPostingWriters[i];
        if (postingWriter->GetDF() > 0)
        {
            InMemFileWriterPtr fileWriterPtr(new InMemFileWriter());
            PostingIteratorPtr postingIter = CreateSinglePostingIterator(fileWriterPtr, mPostingWriters[i], optionFlag, termPayload);
            postingIterators.push_back(make_pair(baseDocid, postingIter));
            fileWriters.push_back(fileWriterPtr);
        }
        baseDocid += mOutputSegmentMergeInfos[i].docCount;
        segmentIdxs.push_back(i);
    }

    MultiSegmentPostingIteratorPtr iter(new MultiSegmentPostingIterator());
    iter->Init(postingIterators, segmentIdxs, fileWriters);
    iter->Reset();
    return iter;
}

uint64_t MultiSegmentBitmapPostingWriter::GetDumpLength(BitmapPostingWriterPtr& postingWriter) const
{
    //total length + postingLength
    return sizeof(uint32_t) + postingWriter->GetDumpLength();
}

uint64_t MultiSegmentBitmapPostingWriter::GetDumpLength() const
{
    uint64_t total = 0;
    for (auto writer : mPostingWriters)
    {
        total += GetDumpLength(writer);
    }
    return total;
}

PostingIteratorPtr MultiSegmentBitmapPostingWriter::CreateSinglePostingIterator(
        InMemFileWriterPtr& fileWriterPtr,
        BitmapPostingWriterPtr& postingWriter,
        optionflag_t optionFlag,
        termpayload_t termPayLoad)
{
    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    fileWriterPtr->Init(GetDumpLength(postingWriter));
    DoDumpPosting(postingWriter, fileWriterPtr, termPayLoad);
    //not using bitmap posting format option because old logic from term extender is same
    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
            postingWriter->GetDF(), postingWriter->GetTotalTF(),
            mPostingFormatOption.GetDocListCompressMode());
    SegmentPosting segPosting(mPostingFormatOption);
    segPosting.Init(compressMode, fileWriterPtr->GetByteSliceList(false),
                    0, SegmentInfo());
    segPostings->push_back(std::move(segPosting));
    
    BitmapPostingIterator* iter = new BitmapPostingIterator(optionFlag);
    iter->Init(segPostings, 1);
    return PostingIteratorPtr(iter);
}




IE_NAMESPACE_END(index);

