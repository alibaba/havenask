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
#include "indexlib/index/inverted_index/truncate/TruncatePostingIteratorCreator.h"

#include "indexlib/index/inverted_index/BufferedPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/truncate/TruncatePostingIterator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, TruncatePostingIteratorCreator);

std::shared_ptr<PostingIterator>
TruncatePostingIteratorCreator::Create(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                       const SegmentPosting& segPosting, bool isBitmap)
{
    auto segPostings = std::make_shared<SegmentPostingVector>();
    segPostings->push_back(segPosting);
    auto iter1 = CreatePostingIterator(indexConfig, segPostings, isBitmap);
    auto iter2 = CreatePostingIterator(indexConfig, segPostings, isBitmap);
    return std::make_shared<TruncatePostingIterator>(iter1, iter2);
}

std::shared_ptr<PostingIterator> TruncatePostingIteratorCreator::Create(const std::shared_ptr<PostingIterator>& it)
{
    std::shared_ptr<PostingIterator> clonedIt(it->Clone());
    return std::make_shared<TruncatePostingIterator>(it, clonedIt);
}

std::shared_ptr<PostingIterator> TruncatePostingIteratorCreator::CreatePostingIterator(
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
    const std::shared_ptr<SegmentPostingVector>& segPostings, bool isBitmap)
{
    auto invertedIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
    assert(invertedIndexConfig != nullptr);
    if (isBitmap) {
        return CreateBitmapPostingIterator(invertedIndexConfig->GetOptionFlag(), segPostings);
    }

    PostingFormatOption postingFormatOption;
    postingFormatOption.Init(invertedIndexConfig);
    auto iter =
        std::make_shared<BufferedPostingIterator>(postingFormatOption, /*sessionPool*/ nullptr, /*tracer*/ nullptr);
    iter->Init(segPostings, NULL, 1);
    return iter;
}

std::shared_ptr<PostingIterator>
TruncatePostingIteratorCreator::CreateBitmapPostingIterator(optionflag_t optionFlag,
                                                            const std::shared_ptr<SegmentPostingVector>& segPostings)
{
    auto iter = std::make_shared<BitmapPostingIterator>(optionFlag);
    iter->Init(segPostings, 1);
    return iter;
}

} // namespace indexlib::index
