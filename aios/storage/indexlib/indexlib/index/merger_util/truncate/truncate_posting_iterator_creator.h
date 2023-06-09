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
#ifndef __INDEXLIB_TRUNCATE_POSTING_ITERATOR_CREATOR_H
#define __INDEXLIB_TRUNCATE_POSTING_ITERATOR_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class TruncatePostingIteratorCreator
{
public:
    TruncatePostingIteratorCreator();
    ~TruncatePostingIteratorCreator();

public:
    static std::shared_ptr<PostingIterator> Create(const config::IndexConfigPtr& indexConfig,
                                                   const SegmentPosting& segPosting, bool isBitmap = false);

    static std::shared_ptr<PostingIterator> Create(const std::shared_ptr<PostingIterator>& it);

private:
    static std::shared_ptr<PostingIterator>
    CreatePostingIterator(const config::IndexConfigPtr& indexConfig,
                          const std::shared_ptr<SegmentPostingVector>& segPostings, bool isBitmap);

    static std::shared_ptr<PostingIterator>
    CreateBitmapPostingIterator(optionflag_t optionFlag, const std::shared_ptr<SegmentPostingVector>& segPostings);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncatePostingIteratorCreator);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_TRUNCATE_POSTING_ITERATOR_CREATOR_H
