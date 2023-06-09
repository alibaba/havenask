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
#include "indexlib/index/inverted_index/PostingMerger.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/MultiSegmentBitmapPostingWriter.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::framework {
struct SegmentMeta;
}

namespace indexlibv2::index {
class DocMapper;
}

namespace indexlib::index {
class SegmentTermInfo;

class BitmapPostingMerger : public PostingMerger
{
public:
    BitmapPostingMerger(autil::mem_pool::Pool* pool,
                        const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments,
                        optionflag_t optionFlag);
    ~BitmapPostingMerger();

public:
    void Merge(const SegmentTermInfos& segTermInfos,
               const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper) override;

    void Dump(const index::DictKeyInfo& key,
              const std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources) override;

    df_t GetDocFreq() override
    {
        if (_df == -1) {
            _df = _writer.GetDF();
        }
        return _df;
    }

    ttf_t GetTotalTF() override { return 0; }

    uint64_t GetPostingLength() const override { return _writer.GetDumpLength(); }

    bool IsCompressedPostingHeader() const override { return false; }

    std::shared_ptr<PostingIterator> CreatePostingIterator() override
    {
        return _writer.CreatePostingIterator(_optionFlag, _termPayload);
    }

private:
    void ApplyPatch(const SegmentTermInfo* segTermInfo, const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper);

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 1024 * 8;
    autil::mem_pool::Pool* _pool;
    MultiSegmentBitmapPostingWriter _writer;
    df_t _df;
    optionflag_t _optionFlag;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
