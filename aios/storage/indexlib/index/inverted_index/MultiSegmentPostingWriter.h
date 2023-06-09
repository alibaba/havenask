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
#include "indexlib/index/inverted_index/PostingWriterImpl.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"

namespace indexlib::file_system {
class InterimFileWriter;
}

namespace indexlib::index {
struct PostingWriterResource;
class IndexOutputSegmentResource;
class PostingIterator;
class PostingWriter;

class MultiSegmentPostingWriter
{
public:
    MultiSegmentPostingWriter(PostingWriterResource* writerResource,
                              const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments,
                              const PostingFormatOption& postingFormatOption)
        : _targetSegments(targetSegments)
    {
        assert(writerResource);
        for (size_t i = 0; i < _targetSegments.size(); i++) {
            std::shared_ptr<PostingWriter> postingWriter(new PostingWriterImpl(writerResource));
            _postingWriters.push_back(postingWriter);
            auto segId = _targetSegments[i]->segmentId;
            _segIdToPostingWriter[segId] = postingWriter;
        }
        _postingFormatOption = postingFormatOption;
    }

    ~MultiSegmentPostingWriter();

public:
    void EndSegment()
    {
        for (size_t i = 0; i < _postingWriters.size(); i++) {
            _postingWriters[i]->EndSegment();
        }
    }

    std::shared_ptr<PostingWriter> GetSegmentPostingWriterBySegId(segmentid_t segId)
    {
        auto iter = _segIdToPostingWriter.find(segId);
        if (iter == _segIdToPostingWriter.end()) {
            return nullptr;
        }
        return iter->second;
    }
    void Dump(const index::DictKeyInfo& key,
              const std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources,
              termpayload_t termPayLoad);

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
    uint64_t GetPostingLength() const
    {
        uint64_t total = 0;
        for (auto postingWriter : _postingWriters) {
            total += postingWriter->GetDumpLength();
        }
        return total;
    }

    std::shared_ptr<PostingIterator> CreatePostingIterator(termpayload_t termPayload);

private:
    std::shared_ptr<PostingIterator>
    CreateSinglePostingIterator(std::shared_ptr<PostingWriter>& postingWriter, termpayload_t termPayload,
                                std::shared_ptr<file_system::InterimFileWriter>& fileWriterPtr);
    dictvalue_t GetDictValue(const std::shared_ptr<PostingWriter>& postingWriter, uint64_t offset);

    void DumpPosting(const index::DictKeyInfo& key, const std::shared_ptr<IndexOutputSegmentResource>& resource,
                     std::shared_ptr<PostingWriter>& postingWriter, termpayload_t termPayload);

    std::shared_ptr<PostingIterator>
    CreatePostingIterator(std::shared_ptr<file_system::InterimFileWriter>& fileWriterPtr,
                          std::shared_ptr<PostingWriter>& postingWriter, termpayload_t termPayload);
    uint64_t GetDumpLength(const std::shared_ptr<PostingWriter>& postingWriter, termpayload_t termPayload) const;
    void DoDumpPosting(std::shared_ptr<PostingWriter>& postingWriter,
                       const std::shared_ptr<file_system::FileWriter>& postingFile, termpayload_t termPayload);

private:
    std::unordered_map<segmentid_t, std::shared_ptr<PostingWriter>> _segIdToPostingWriter;
    std::vector<std::shared_ptr<PostingWriter>> _postingWriters;
    PostingFormatOption _postingFormatOption;
    std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>> _targetSegments;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
