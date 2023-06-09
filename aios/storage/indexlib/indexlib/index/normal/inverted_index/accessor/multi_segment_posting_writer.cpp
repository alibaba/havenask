/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_writer.h"

#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/util/NumericUtil.h"

using namespace std;
using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, MultiSegmentPostingWriter);
DECLARE_REFERENCE_CLASS(index, IndexOutputSegmentResource);

MultiSegmentPostingWriter::~MultiSegmentPostingWriter() {}

void MultiSegmentPostingWriter::Dump(const index::DictKeyInfo& key,
                                     IndexOutputSegmentResources& indexOutputSegmentResources,
                                     termpayload_t termPayload)
{
    assert(indexOutputSegmentResources.size() == mOutputSegmentMergeInfos.size());
    for (size_t i = 0; i < indexOutputSegmentResources.size(); i++) {
        DumpPosting(key, indexOutputSegmentResources[i], mPostingWriters[i], termPayload);
    }
}

dictvalue_t MultiSegmentPostingWriter::GetDictValue(const shared_ptr<PostingWriter>& postingWriter, uint64_t offset)
{
    uint64_t inlinePostingValue;
    bool isDocList = false;
    if (postingWriter->GetDictInlinePostingValue(inlinePostingValue, isDocList)) {
        return ShortListOptimizeUtil::CreateDictInlineValue(inlinePostingValue, isDocList, true /*dfFirst*/);
    }
    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(postingWriter->GetDF(), postingWriter->GetTotalTF(),
                                                                  mPostingFormatOption.GetDocListCompressMode());
    return ShortListOptimizeUtil::CreateDictValue(compressMode, offset);
}

std::shared_ptr<PostingIterator> MultiSegmentPostingWriter::CreatePostingIterator(termpayload_t termPayload)
{
    vector<MultiSegmentPostingIterator::PostingIteratorInfo> postingIterators;
    vector<file_system::InterimFileWriterPtr> fileWriters;
    vector<segmentid_t> segmentIdxs;
    int64_t baseDocid = 0;
    for (int32_t i = 0; i < (int32_t)mPostingWriters.size(); i++) {
        auto postingWriter = mPostingWriters[i];
        if (postingWriter->GetDF() > 0) {
            file_system::InterimFileWriterPtr fileWriterPtr(new file_system::InterimFileWriter());
            std::shared_ptr<PostingIterator> postingIter =
                CreateSinglePostingIterator(postingWriter, termPayload, fileWriterPtr);
            postingIterators.push_back(make_pair(baseDocid, postingIter));
            fileWriters.push_back(fileWriterPtr);
            segmentIdxs.push_back(i);
        }
        baseDocid += mOutputSegmentMergeInfos[i].docCount;
    }

    std::shared_ptr<MultiSegmentPostingIterator> iter(new MultiSegmentPostingIterator());
    iter->Init(postingIterators, segmentIdxs, fileWriters);
    iter->Reset();
    return iter;
}

std::shared_ptr<PostingIterator>
MultiSegmentPostingWriter::CreateSinglePostingIterator(shared_ptr<PostingWriter>& postingWriter,
                                                       termpayload_t termPayload,
                                                       file_system::InterimFileWriterPtr& fileWriterPtr)
{
    fileWriterPtr->Init(GetDumpLength(postingWriter, termPayload));
    std::shared_ptr<PostingIterator> postingIter = CreatePostingIterator(fileWriterPtr, postingWriter, termPayload);
    return postingIter;
}

std::shared_ptr<PostingIterator>
MultiSegmentPostingWriter::CreatePostingIterator(file_system::InterimFileWriterPtr& fileWriterPtr,
                                                 shared_ptr<PostingWriter>& postingWriter, termpayload_t termPayload)
{
    uint64_t dictInlineValue;
    shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    bool isDocList = false;
    bool dfFirst = true;
    if (postingWriter->GetDictInlinePostingValue(dictInlineValue, isDocList)) {
        SegmentPosting segPosting(mPostingFormatOption);
        segPosting.Init((docid_t)0, (uint64_t)0,
                        ShortListOptimizeUtil::CreateDictInlineValue(dictInlineValue, isDocList, dfFirst), isDocList,
                        dfFirst);
        segPostings->push_back(std::move(segPosting));
    } else {
        DoDumpPosting<file_system::InterimFileWriterPtr>(postingWriter, fileWriterPtr, termPayload);
        uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(
            postingWriter->GetDF(), postingWriter->GetTotalTF(), mPostingFormatOption.GetDocListCompressMode());
        SegmentPosting segPosting(mPostingFormatOption);
        segPosting.Init(compressMode, fileWriterPtr->GetByteSliceList(true), 0, 0);
        segPostings->push_back(std::move(segPosting));
    }

    BufferedPostingIterator* bufferedPostingIter =
        new BufferedPostingIterator(mPostingFormatOption, /*sessionPool*/ nullptr, /*tracer*/ nullptr);
    bufferedPostingIter->Init(segPostings, NULL, 1);
    std::shared_ptr<PostingIterator> postingIter(bufferedPostingIter);
    return postingIter;
}

uint64_t MultiSegmentPostingWriter::GetDumpLength(const shared_ptr<PostingWriter>& postingWriter,
                                                  termpayload_t termPayload) const
{
    TermMeta termMeta(postingWriter->GetDF(), postingWriter->GetTotalTF(), termPayload);
    TermMetaDumper tmDumper(mPostingFormatOption);
    uint64_t length = tmDumper.CalculateStoreSize(termMeta) + postingWriter->GetDumpLength();
    if (mPostingFormatOption.IsCompressedPostingHeader()) {
        length += VByteCompressor::GetVInt32Length(length);
    } else {
        length += sizeof(uint32_t);
    }
    return length;
}

void MultiSegmentPostingWriter::DumpPosting(const index::DictKeyInfo& key, IndexOutputSegmentResourcePtr& resource,
                                            shared_ptr<PostingWriter>& postingWriter, termpayload_t termPayload)
{
    if (postingWriter->GetDF() <= 0) {
        return;
    }
    std::shared_ptr<IndexDataWriter> termDataWriter = resource->GetIndexDataWriter(SegmentTermInfo::TM_NORMAL);
    auto postingFile = termDataWriter->postingWriter;
    dictvalue_t dictValue = GetDictValue(postingWriter, postingFile->GetLogicLength());
    termDataWriter->dictWriter->AddItem(key, dictValue);
    bool isDocList = false;
    bool dfFirst = true;
    if (ShortListOptimizeUtil::IsDictInlineCompressMode(dictValue, isDocList, dfFirst)) {
        return;
    }

    // dump posting
    DoDumpPosting<file_system::FileWriterPtr>(postingWriter, postingFile, termPayload);
}
}}} // namespace indexlib::index::legacy
