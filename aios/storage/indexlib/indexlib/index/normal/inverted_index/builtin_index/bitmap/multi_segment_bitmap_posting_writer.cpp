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
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/multi_segment_bitmap_posting_writer.h"

#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index_base/index_meta/segment_info.h"

using namespace std;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, MultiSegmentBitmapPostingWriter);

using namespace indexlib::index_base;

MultiSegmentBitmapPostingWriter::~MultiSegmentBitmapPostingWriter() {}

void MultiSegmentBitmapPostingWriter::Dump(const index::DictKeyInfo& key,
                                           IndexOutputSegmentResources& indexOutputSegmentResources,
                                           termpayload_t termPayLoad)
{
    assert(indexOutputSegmentResources.size() == mOutputSegmentMergeInfos.size());
    for (size_t i = 0; i < indexOutputSegmentResources.size(); i++) {
        DumpPosting(key, indexOutputSegmentResources[i], mPostingWriters[i], termPayLoad);
    }
}

void MultiSegmentBitmapPostingWriter::DumpPosting(const index::DictKeyInfo& key,
                                                  IndexOutputSegmentResourcePtr& resource,
                                                  std::shared_ptr<PostingWriter> postingWriter,
                                                  termpayload_t termPayload)
{
    if (postingWriter->GetDF() <= 0) {
        return;
    }

    std::shared_ptr<IndexDataWriter> termDataWriter = resource->GetIndexDataWriter(SegmentTermInfo::TM_BITMAP);
    auto postingFile = termDataWriter->postingWriter;
    dictvalue_t dictValue = postingFile->GetLogicLength();
    termDataWriter->dictWriter->AddItem(key, dictValue);
    postingWriter->SetTermPayload(termPayload);
    uint32_t totalLength = postingWriter->GetDumpLength();
    postingFile->Write((void*)&totalLength, sizeof(uint32_t)).GetOrThrow();
    postingWriter->Dump(postingFile);
}

std::shared_ptr<PostingIterator> MultiSegmentBitmapPostingWriter::CreatePostingIterator(optionflag_t optionFlag,
                                                                                        termpayload_t termPayload)
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
                CreateSinglePostingIterator(fileWriterPtr, mPostingWriters[i], optionFlag, termPayload);
            postingIterators.push_back(make_pair(baseDocid, postingIter));
            fileWriters.push_back(fileWriterPtr);
        }
        baseDocid += mOutputSegmentMergeInfos[i].docCount;
        segmentIdxs.push_back(i);
    }

    std::shared_ptr<MultiSegmentPostingIterator> iter(new MultiSegmentPostingIterator());
    iter->Init(postingIterators, segmentIdxs, fileWriters);
    iter->Reset();
    return iter;
}

uint64_t MultiSegmentBitmapPostingWriter::GetDumpLength(std::shared_ptr<BitmapPostingWriter>& postingWriter) const
{
    // total length + postingLength
    return sizeof(uint32_t) + postingWriter->GetDumpLength();
}

uint64_t MultiSegmentBitmapPostingWriter::GetDumpLength() const
{
    uint64_t total = 0;
    for (auto writer : mPostingWriters) {
        total += GetDumpLength(writer);
    }
    return total;
}

std::shared_ptr<PostingIterator>
MultiSegmentBitmapPostingWriter::CreateSinglePostingIterator(file_system::InterimFileWriterPtr& fileWriterPtr,
                                                             std::shared_ptr<BitmapPostingWriter>& postingWriter,
                                                             optionflag_t optionFlag, termpayload_t termPayLoad)
{
    std::shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    fileWriterPtr->Init(GetDumpLength(postingWriter));
    DoDumpPosting(postingWriter, fileWriterPtr, termPayLoad);
    // not using bitmap posting format option because old logic from term extender is same
    uint8_t compressMode = ShortListOptimizeUtil::GetCompressMode(postingWriter->GetDF(), postingWriter->GetTotalTF(),
                                                                  mPostingFormatOption.GetDocListCompressMode());
    SegmentPosting segPosting(mPostingFormatOption);
    segPosting.Init(compressMode, fileWriterPtr->GetByteSliceList(false), 0, 0);
    segPostings->push_back(std::move(segPosting));

    BitmapPostingIterator* iter = new BitmapPostingIterator(optionFlag);
    iter->Init(segPostings, 1);
    return std::shared_ptr<PostingIterator>(iter);
}
}}} // namespace indexlib::index::legacy
