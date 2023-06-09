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

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib::index {

class TruncatePostingIteratorCreator
{
public:
    TruncatePostingIteratorCreator() = default;
    ~TruncatePostingIteratorCreator() = default;

public:
    static std::shared_ptr<PostingIterator> Create(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                                   const SegmentPosting& segPosting, bool isBitmap = false);

    static std::shared_ptr<PostingIterator> Create(const std::shared_ptr<PostingIterator>& it);

private:
    static std::shared_ptr<PostingIterator>
    CreatePostingIterator(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                          const std::shared_ptr<SegmentPostingVector>& segPostings, bool isBitmap);

    static std::shared_ptr<PostingIterator>
    CreateBitmapPostingIterator(optionflag_t optionFlag, const std::shared_ptr<SegmentPostingVector>& segPostings);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
