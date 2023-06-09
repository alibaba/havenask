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
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/SegmentTermInfo.h"

namespace indexlib::util {
class DictKeyInfo;
}
namespace indexlibv2::index {
class DocMapper;
}

namespace indexlib::index {
class PostingIterator;

class PostingMerger
{
public:
    PostingMerger() : _termPayload(0) {}
    virtual ~PostingMerger() = default;

    virtual void Merge(const SegmentTermInfos& segTermInfos,
                       const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper) = 0;
    virtual void Dump(const index::DictKeyInfo& key,
                      const std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources) = 0;

    virtual df_t GetDocFreq() = 0;
    virtual ttf_t GetTotalTF() = 0;

    virtual uint64_t GetPostingLength() const = 0;
    virtual bool GetDictInlinePostingValue(uint64_t& inlinePostingValue) { return false; }

    virtual bool IsCompressedPostingHeader() const = 0;
    termpayload_t GetTermPayload() const { return _termPayload; }

    virtual std::shared_ptr<PostingIterator> CreatePostingIterator() = 0;

protected:
    termpayload_t _termPayload;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
