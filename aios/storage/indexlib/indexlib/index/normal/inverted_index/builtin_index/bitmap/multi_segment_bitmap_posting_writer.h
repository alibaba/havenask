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
#ifndef __INDEXLIB_MULTI_SEGMENT_BITMAP_POSTING_WRITER_H
#define __INDEXLIB_MULTI_SEGMENT_BITMAP_POSTING_WRITER_H

#include <memory>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/file_system/file/InterimFileWriter.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/MultiSegmentPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"

namespace indexlib { namespace index { namespace legacy {

class MultiSegmentBitmapPostingWriter
{
public:
    MultiSegmentBitmapPostingWriter(autil::mem_pool::PoolBase* pool,
                                    const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
    {
        for (size_t i = 0; i < outputSegmentMergeInfos.size(); i++) {
            std::shared_ptr<BitmapPostingWriter> writer(new BitmapPostingWriter(pool));
            mPostingWriters.push_back(writer);
        }
        mOutputSegmentMergeInfos = outputSegmentMergeInfos;
    }
    ~MultiSegmentBitmapPostingWriter();

public:
    void EndSegment()
    {
        for (auto writer : mPostingWriters) {
            writer->EndSegment();
        }
    }

    std::shared_ptr<PostingWriter> GetSegmentBitmapPostingWriter(size_t segmentIdx)
    {
        if (segmentIdx >= mPostingWriters.size()) {
            return std::shared_ptr<PostingWriter>();
        }
        return mPostingWriters[segmentIdx];
    }
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

    void Dump(const index::DictKeyInfo& key, IndexOutputSegmentResources& indexOutputSegmentResources,
              termpayload_t termPayLoad);

    std::shared_ptr<PostingIterator> CreatePostingIterator(optionflag_t optionFlag, termpayload_t termPayload);
    uint64_t GetDumpLength() const;

private:
    void DumpPosting(const index::DictKeyInfo& key, IndexOutputSegmentResourcePtr& resource,
                     std::shared_ptr<PostingWriter> postingWriter, termpayload_t termPayload);
    template <typename FileWriterPtr>
    void DoDumpPosting(std::shared_ptr<PostingWriter> postingWriter, FileWriterPtr& fileWriterPtr,
                       termpayload_t termPayload);

    std::shared_ptr<PostingIterator> CreateSinglePostingIterator(file_system::InterimFileWriterPtr& fileWriterPtr,
                                                                 std::shared_ptr<BitmapPostingWriter>& postingWriter,
                                                                 optionflag_t optionFlag, termpayload_t termPayLoad);
    uint64_t GetDumpLength(std::shared_ptr<BitmapPostingWriter>& postingWriter) const;

public:
    std::vector<std::shared_ptr<BitmapPostingWriter>> mPostingWriters;
    PostingFormatOption mPostingFormatOption;
    index_base::OutputSegmentMergeInfos mOutputSegmentMergeInfos;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiSegmentBitmapPostingWriter);
///////////////////////////////////////////////////////////
template <typename FileWriterPtr>
void MultiSegmentBitmapPostingWriter::DoDumpPosting(std::shared_ptr<PostingWriter> postingWriter,
                                                    FileWriterPtr& fileWriterPtr, termpayload_t termPayload)
{
    // std::shared_ptr<IndexDataWriter> termDataWriter =
    // resource.GetIndexDataWriter(SegmentTermInfo::TM_BITMAP); auto postingFile = termDataWriter->postingWriter;
    postingWriter->SetTermPayload(termPayload);
    uint32_t totalLength = postingWriter->GetDumpLength();
    fileWriterPtr->Write((void*)&totalLength, sizeof(uint32_t)).GetOrThrow();
    postingWriter->Dump(fileWriterPtr);
}
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_MULTI_SEGMENT_BITMAP_POSTING_WRITER_H
