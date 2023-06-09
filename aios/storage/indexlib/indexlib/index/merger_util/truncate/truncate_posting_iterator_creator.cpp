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
#include "indexlib/index/merger_util/truncate/truncate_posting_iterator_creator.h"

#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/merger_util/truncate/truncate_posting_iterator.h"

using namespace std;

using namespace indexlib::config;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, TruncatePostingIteratorCreator);

TruncatePostingIteratorCreator::TruncatePostingIteratorCreator() {}

TruncatePostingIteratorCreator::~TruncatePostingIteratorCreator() {}

std::shared_ptr<PostingIterator> TruncatePostingIteratorCreator::Create(const IndexConfigPtr& indexConfig,
                                                                        const SegmentPosting& segPosting, bool isBitmap)
{
    shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    segPostings->push_back(segPosting);

    std::shared_ptr<PostingIterator> iter1 = CreatePostingIterator(indexConfig, segPostings, isBitmap);
    std::shared_ptr<PostingIterator> iter2 = CreatePostingIterator(indexConfig, segPostings, isBitmap);
    TruncatePostingIterator* iter = new TruncatePostingIterator(iter1, iter2);
    return std::shared_ptr<PostingIterator>(iter);
}

std::shared_ptr<PostingIterator> TruncatePostingIteratorCreator::Create(const std::shared_ptr<PostingIterator>& it)
{
    std::shared_ptr<PostingIterator> clonedIt(it->Clone());

    TruncatePostingIterator* iter = new TruncatePostingIterator(it, clonedIt);
    return std::shared_ptr<PostingIterator>(iter);
}

std::shared_ptr<PostingIterator> TruncatePostingIteratorCreator::CreatePostingIterator(
    const IndexConfigPtr& indexConfig, const shared_ptr<SegmentPostingVector>& segPostings, bool isBitmap)
{
    if (isBitmap) {
        return CreateBitmapPostingIterator(indexConfig->GetOptionFlag(), segPostings);
    }

    PostingFormatOption postingFormatOption;
    postingFormatOption.Init(indexConfig);
    BufferedPostingIterator* iter =
        new BufferedPostingIterator(postingFormatOption, /*sessionPool*/ nullptr, /*tracer*/ nullptr);
    iter->Init(segPostings, NULL, 1);
    return std::shared_ptr<PostingIterator>(iter);
}

std::shared_ptr<PostingIterator>
TruncatePostingIteratorCreator::CreateBitmapPostingIterator(optionflag_t optionFlag,
                                                            const shared_ptr<SegmentPostingVector>& segPostings)
{
    BitmapPostingIterator* iter = new BitmapPostingIterator(optionFlag);
    iter->Init(segPostings, 1);
    return std::shared_ptr<PostingIterator>(iter);
}
} // namespace indexlib::index::legacy
