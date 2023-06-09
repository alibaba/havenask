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
#include "indexlib/index/normal/inverted_index/accessor/index_term_extender.h"

#include "indexlib/config/index_config.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/merger_util/truncate/truncate_posting_iterator.h"
#include "indexlib/index/merger_util/truncate/truncate_posting_iterator_creator.h"
#include "indexlib/index/normal/adaptive_bitmap/multi_adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_merger.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, IndexTermExtender);

IndexTermExtender::IndexTermExtender(const config::IndexConfigPtr& indexConfig,
                                     const TruncateIndexWriterPtr& truncateIndexWriter,
                                     const legacy::MultiAdaptiveBitmapIndexWriterPtr& adaptiveBitmapWriter)
    : mIndexConfig(indexConfig)
    , mTruncateIndexWriter(truncateIndexWriter)
    , mAdaptiveBitmapIndexWriter(adaptiveBitmapWriter)
{
    mIndexFormatOption.Init(indexConfig);
}

IndexTermExtender::~IndexTermExtender() {}

void IndexTermExtender::Init(const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                             IndexOutputSegmentResources& indexOutputSegmentResources)
{
    if (mTruncateIndexWriter) {
        mTruncateIndexWriter->Init(outputSegMergeInfos);
    }
    if (mAdaptiveBitmapIndexWriter) {
        mAdaptiveBitmapIndexWriter->Init(indexOutputSegmentResources);
    }
}

IndexTermExtender::TermOperation IndexTermExtender::ExtendTerm(index::DictKeyInfo key,
                                                               const PostingMergerPtr& postingMerger,
                                                               SegmentTermInfo::TermIndexMode mode)
{
    bool truncateRemain = true;
    bool adaptiveRemain = true;
    if (mTruncateIndexWriter) {
        truncateRemain = MakeTruncateTermPosting(key, postingMerger, mode);
    }

    if (mAdaptiveBitmapIndexWriter && mode != SegmentTermInfo::TM_BITMAP) {
        adaptiveRemain = MakeAdaptiveBitmapTermPosting(key, postingMerger);
    }

    if (truncateRemain && adaptiveRemain) {
        return TO_REMAIN;
    }
    return TO_DISCARD;
}

void IndexTermExtender::Destroy()
{
    if (mAdaptiveBitmapIndexWriter) {
        mAdaptiveBitmapIndexWriter->EndPosting();
    }

    if (mTruncateIndexWriter) {
        mTruncateIndexWriter->EndPosting();
    }
}

bool IndexTermExtender::MakeTruncateTermPosting(index::DictKeyInfo key, const PostingMergerPtr& postingMerger,
                                                SegmentTermInfo::TermIndexMode mode)
{
    bool remainPosting = true;
    df_t df = postingMerger->GetDocFreq();
    TruncateTriggerInfo triggerInfo(key, df);
    if (mTruncateIndexWriter->NeedTruncate(triggerInfo)) {
        if (mode == SegmentTermInfo::TM_NORMAL && mIndexConfig->IsBitmapOnlyTerm(key)) {
            // fixbug #258648
            int32_t truncPostingCount = mTruncateIndexWriter->GetEstimatePostingCount();
            int32_t truncMetaPostingCount = 0;
            mIndexConfig->GetTruncatePostingCount(key, truncMetaPostingCount);
            if (truncPostingCount >= truncMetaPostingCount) {
                // only bitmap && normal term only exist in no truncate posting segment
                remainPosting = false;
            }
            // Do not do truncate, bitmap term will cover
            return remainPosting;
        }

        std::shared_ptr<PostingIterator> postingIterator = postingMerger->CreatePostingIterator();
        std::shared_ptr<PostingIterator> postingIteratorClone(postingIterator->Clone());
        TruncatePostingIteratorPtr truncatePostingIterator(
            new TruncatePostingIterator(postingIterator, postingIteratorClone));
        const string& indexName = mIndexConfig->GetIndexName();
        IE_LOG(DEBUG, "Begin generating truncate list for %s:%s", indexName.c_str(), key.ToString().c_str());
        mTruncateIndexWriter->AddPosting(key, truncatePostingIterator, postingMerger->GetDocFreq());
        IE_LOG(DEBUG, "End generating truncate list for %s:%s", indexName.c_str(), key.ToString().c_str());
    }
    return remainPosting;
}

bool IndexTermExtender::MakeAdaptiveBitmapTermPosting(index::DictKeyInfo key, const PostingMergerPtr& postingMerger)
{
    bool isAdaptive = mAdaptiveBitmapIndexWriter->NeedAdaptiveBitmap(key, postingMerger);

    bool remainPosting = true;
    if (isAdaptive) {
        std::shared_ptr<PostingIterator> postingIter = postingMerger->CreatePostingIterator();
        assert(postingIter);

        const string& indexName = mIndexConfig->GetIndexName();
        IE_LOG(DEBUG, "Begin generating adaptive bitmap list for %s:%s", indexName.c_str(), key.ToString().c_str());
        mAdaptiveBitmapIndexWriter->AddPosting(key, postingMerger->GetTermPayload(), postingIter);
        IE_LOG(DEBUG, "End generating adaptive bitmap list for %s:%s", indexName.c_str(), key.ToString().c_str());

        if (mIndexConfig->GetHighFrequencyTermPostingType() == indexlib::index::hp_bitmap) {
            remainPosting = false;
        }
    }
    return remainPosting;
}

std::shared_ptr<PostingIterator> IndexTermExtender::CreateCommonPostingIterator(const util::ByteSliceListPtr& sliceList,
                                                                                uint8_t compressMode,
                                                                                SegmentTermInfo::TermIndexMode mode)
{
    PostingFormatOption postingFormatOption = mIndexFormatOption.GetPostingFormatOption();
    if (mode == SegmentTermInfo::TM_BITMAP) {
        postingFormatOption = postingFormatOption.GetBitmapPostingFormatOption();
    }

    SegmentPosting segPosting(postingFormatOption);
    segPosting.Init(compressMode, sliceList, 0, 0);

    return TruncatePostingIteratorCreator::Create(mIndexConfig, segPosting, mode == SegmentTermInfo::TM_BITMAP);
}
}}} // namespace indexlib::index::legacy
