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
#include "indexlib/index/inverted_index/MultiSegmentPostingWriter.h"
#include "indexlib/index/inverted_index/PostingMerger.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlibv2::framework {
struct SegmentMeta;
}

namespace indexlib::index {
class MultiSegmentPostingWriter;

class PostingDecoderImpl;
struct PostingWriterResource;
class SingleTermIndexSegmentPatchIterator;
class PostingIterator;
class PostingMergerImpl : public PostingMerger
{
public:
    PostingMergerImpl(PostingWriterResource* postingWriterResource,
                      const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);
    ~PostingMergerImpl() = default;

public:
    void Merge(const SegmentTermInfos& segTermInfos,
               const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper) override;

    void Dump(const index::DictKeyInfo& key,
              const std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources) override;

    uint64_t GetPostingLength() const override;

    ttf_t GetTotalTF() override
    {
        if (_ttf == -1) {
            _ttf = _postingWriter->GetTotalTF();
        }
        return _ttf;
    }

    df_t GetDocFreq() override
    {
        if (_df == -1) {
            _df = _postingWriter->GetDF();
        }
        return _df;
    }

    bool IsCompressedPostingHeader() const override { return true; }

private:
    void MergeOneSegment(const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper, PostingDecoderImpl* decoder,
                         SingleTermIndexSegmentPatchIterator* patchIter, docid_t baseDocId);

    std::shared_ptr<MultiSegmentPostingWriter>
    CreatePostingWriter(PostingWriterResource* postingWriterResource,
                        const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments);

    std::shared_ptr<PostingIterator> CreatePostingIterator() override
    {
        return _postingWriter->CreatePostingIterator(_termPayload);
    }

private:
    PostingFormatOption _postingFormatOption;
    std::shared_ptr<MultiSegmentPostingWriter> _postingWriter;
    df_t _df;
    ttf_t _ttf;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
