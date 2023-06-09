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

#include "autil/Log.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib::file_system {
class InterimFileWriter;
}

namespace indexlib::index {
struct PostingWriterResource;
class IndexOutputSegmentResource;
class PostingIterator;
class PostingWriter;

class MultiSegmentBitmapPostingWriter
{
public:
    MultiSegmentBitmapPostingWriter(
        autil::mem_pool::PoolBase* pool,
        const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments)
    {
        for (size_t i = 0; i < targetSegments.size(); i++) {
            auto writer = std::make_shared<BitmapPostingWriter>(pool);
            _postingWriters.push_back(writer);
            auto segId = targetSegments[i]->segmentId;
            _segIdToPostingWriter[segId] = writer;
        }
        _targetSegments = targetSegments;
    }
    ~MultiSegmentBitmapPostingWriter() = default;

public:
    void EndSegment()
    {
        for (auto writer : _postingWriters) {
            writer->EndSegment();
        }
    }

    std::shared_ptr<PostingWriter> GetSegmentBitmapPostingWriterBySegId(segmentid_t segId)
    {
        auto iter = _segIdToPostingWriter.find(segId);
        if (iter == _segIdToPostingWriter.end()) {
            return nullptr;
        }
        return iter->second;
    }
    df_t GetDF() const
    {
        df_t total = 0;
        for (auto postingWriter : _postingWriters) {
            total += postingWriter->GetDF();
        }
        return total;
    }
    ttf_t GetTotalTF() const
    {
        ttf_t total = 0;
        for (auto postingWriter : _postingWriters) {
            total += postingWriter->GetTotalTF();
        }
        return total;
    }

    void Dump(const index::DictKeyInfo& key,
              const std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources,
              termpayload_t termPayLoad);

    std::shared_ptr<PostingIterator> CreatePostingIterator(optionflag_t optionFlag, termpayload_t termPayload);
    uint64_t GetDumpLength() const;

private:
    void DumpPosting(const index::DictKeyInfo& key, const std::shared_ptr<IndexOutputSegmentResource>& resource,
                     std::shared_ptr<PostingWriter> postingWriter, termpayload_t termPayload);
    template <typename FileWriterPtr>
    void DoDumpPosting(std::shared_ptr<PostingWriter> postingWriter, FileWriterPtr& fileWriterPtr,
                       termpayload_t termPayload);

    std::shared_ptr<PostingIterator>
    CreateSinglePostingIterator(std::shared_ptr<file_system::InterimFileWriter>& fileWriterPtr,
                                std::shared_ptr<BitmapPostingWriter>& postingWriter, optionflag_t optionFlag,
                                termpayload_t termPayLoad);
    uint64_t GetDumpLength(const std::shared_ptr<BitmapPostingWriter>& postingWriter) const;

    std::unordered_map<segmentid_t, std::shared_ptr<PostingWriter>> _segIdToPostingWriter;
    std::vector<std::shared_ptr<BitmapPostingWriter>> _postingWriters;
    PostingFormatOption _postingFormatOption;
    std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>> _targetSegments;

    AUTIL_LOG_DECLARE();
};

template <typename FileWriterPtr>
void MultiSegmentBitmapPostingWriter::DoDumpPosting(std::shared_ptr<PostingWriter> postingWriter,
                                                    FileWriterPtr& fileWriterPtr, termpayload_t termPayload)
{
    // std::shared_ptr<indexlibv2::index::IndexDataWriter> termDataWriter =
    // resource.GetIndexDataWriter(SegmentTermInfo::TM_BITMAP); auto postingFile = termDataWriter->postingWriter;
    postingWriter->SetTermPayload(termPayload);
    uint32_t totalLength = postingWriter->GetDumpLength();
    fileWriterPtr->Write((void*)&totalLength, sizeof(uint32_t)).GetOrThrow();
    postingWriter->Dump(fileWriterPtr);
}

} // namespace indexlib::index
