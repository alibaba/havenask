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
#include "indexlib/index/inverted_index/SegmentTermInfo.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::index {
class TruncateIndexWriter;
class MultiAdaptiveBitmapIndexWriter;
class IndexOutputSegmentResource;
class PostingIterator;
} // namespace indexlib::index

namespace indexlib::util {
class DictKeyInfo;
class ByteSliceList;
} // namespace indexlib::util

namespace indexlibv2::framework {
struct SegmentMeta;
}

namespace indexlib::index {
class PostingMerger;

class IndexTermExtender
{
public:
    enum TermOperation { TO_REMAIN, TO_DISCARD };

    IndexTermExtender(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                      const std::shared_ptr<indexlib::index::TruncateIndexWriter>& truncateIndexWriter,
                      const std::shared_ptr<MultiAdaptiveBitmapIndexWriter>& adaptiveBitmapWriter);

    virtual ~IndexTermExtender() = default;

    void Init(const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments,
              std::vector<std::shared_ptr<indexlib::index::IndexOutputSegmentResource>>& indexOutputSegmentResources);

    std::pair<Status, TermOperation> ExtendTerm(DictKeyInfo key, const std::shared_ptr<PostingMerger>& postingMerger,
                                                SegmentTermInfo::TermIndexMode mode);
    void Destroy();

private:
    std::pair<Status, bool> MakeTruncateTermPosting(indexlib::index::DictKeyInfo key,
                                                    const std::shared_ptr<PostingMerger>& postingMerger,
                                                    indexlib::index::SegmentTermInfo::TermIndexMode mode);

    bool MakeAdaptiveBitmapTermPosting(indexlib::index::DictKeyInfo key,
                                       const std::shared_ptr<PostingMerger>& postingMerger);

    // virtual for UT
    virtual std::shared_ptr<indexlib::index::PostingIterator>
    CreateCommonPostingIterator(const std::shared_ptr<indexlib::util::ByteSliceList>& sliceList, uint8_t compressMode,
                                indexlib::index::SegmentTermInfo::TermIndexMode mode);

private:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    std::shared_ptr<TruncateIndexWriter> _truncateIndexWriter;
    std::shared_ptr<MultiAdaptiveBitmapIndexWriter> _adaptiveBitmapIndexWriter;
    IndexFormatOption _indexFormatOption;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
