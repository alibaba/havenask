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
#pragma once

#include <memory>

#include "indexlib/file_system/file/InterimFileWriter.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/MultiSegmentPostingIterator.h"
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"

namespace indexlib { namespace index { namespace legacy {

class MultiSegmentPostingWriter
{
public:
    MultiSegmentPostingWriter(PostingWriterResource* writerResource,
                              const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos,
                              PostingFormatOption& postingFormatOption)
    {
        assert(writerResource);
        mOutputSegmentMergeInfos = outputSegmentMergeInfos;
        for (size_t i = 0; i < outputSegmentMergeInfos.size(); i++) {
            std::shared_ptr<PostingWriter> postingWriter(new PostingWriterImpl(writerResource));
            mPostingWriters.push_back(postingWriter);
        }
        mPostingFormatOption = postingFormatOption;
    }

    ~MultiSegmentPostingWriter();

public:
    void EndSegment()
    {
        for (size_t i = 0; i < mPostingWriters.size(); i++) {
            mPostingWriters[i]->EndSegment();
        }
    }

    std::shared_ptr<PostingWriter> GetSegmentPostingWriter(size_t segmentIdx)
    {
        if (segmentIdx >= mPostingWriters.size()) {
            return std::shared_ptr<PostingWriter>();
        }
        return mPostingWriters[segmentIdx];
    }

    void Dump(const index::DictKeyInfo& key, IndexOutputSegmentResources& indexOutputSegmentResources,
              termpayload_t termPayLoad);

    df_t GetDF() const
    {
        df_t total = 0;
        for (auto postingWriter : mPostingWriters) {
            total += postingWriter->GetDF();
        }
        return total;
    }
    ttf_t GetTotalTF() const
    {
        ttf_t total = 0;
        for (auto postingWriter : mPostingWriters) {
            total += postingWriter->GetTotalTF();
        }
        return total;
    }
    uint64_t GetPostingLength() const
    {
        uint64_t total = 0;
        for (auto postingWriter : mPostingWriters) {
            total += postingWriter->GetDumpLength();
        }
        return total;
    }

    std::shared_ptr<PostingIterator> CreatePostingIterator(termpayload_t termPayload);

private:
    std::shared_ptr<PostingIterator> CreateSinglePostingIterator(std::shared_ptr<PostingWriter>& postingWriter,
                                                                 termpayload_t termPayload,
                                                                 file_system::InterimFileWriterPtr& fileWriterPtr);
    dictvalue_t GetDictValue(const std::shared_ptr<PostingWriter>& postingWriter, uint64_t offset);

    void DumpPosting(const index::DictKeyInfo& key, IndexOutputSegmentResourcePtr& resource,
                     std::shared_ptr<PostingWriter>& postingWriter, termpayload_t termPayload);

    std::shared_ptr<PostingIterator> CreatePostingIterator(file_system::InterimFileWriterPtr& fileWriterPtr,
                                                           std::shared_ptr<PostingWriter>& postingWriter,
                                                           termpayload_t termPayload);
    uint64_t GetDumpLength(const std::shared_ptr<PostingWriter>& postingWriter, termpayload_t termPayload) const;
    template <typename FileWriterPtr>
    void DoDumpPosting(std::shared_ptr<PostingWriter>& postingWriter, FileWriterPtr& postingFile,
                       termpayload_t termPayload);

public:
    std::vector<std::shared_ptr<PostingWriter>> mPostingWriters;
    PostingFormatOption mPostingFormatOption;
    index_base::OutputSegmentMergeInfos mOutputSegmentMergeInfos;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiSegmentPostingWriter);
///////////////////////////////////////////////////////////////////
template <typename FileWriterPtr>
void MultiSegmentPostingWriter::DoDumpPosting(std::shared_ptr<PostingWriter>& postingWriter, FileWriterPtr& postingFile,
                                              termpayload_t termPayload)
{
    TermMeta termMeta(postingWriter->GetDF(), postingWriter->GetTotalTF(), termPayload);
    TermMetaDumper tmDumper(mPostingFormatOption);

    uint32_t totalLen = tmDumper.CalculateStoreSize(termMeta) + postingWriter->GetDumpLength();
    if (mPostingFormatOption.IsCompressedPostingHeader()) {
        postingFile->WriteVUInt32(totalLen).GetOrThrow();
    } else {
        postingFile->Write((const void*)&totalLen, sizeof(uint32_t)).GetOrThrow();
    }
    tmDumper.Dump(postingFile, termMeta);
    postingWriter->Dump(postingFile);
}
}}} // namespace indexlib::index::legacy
