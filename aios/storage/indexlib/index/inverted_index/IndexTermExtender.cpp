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
#include "indexlib/index/inverted_index/IndexTermExtender.h"

#include "indexlib/base/Status.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/inverted_index/IndexOutputSegmentResource.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/PostingMergerImpl.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/MultiAdaptiveBitmapIndexWriter.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/truncate/TruncateIndexWriter.h"
#include "indexlib/index/inverted_index/truncate/TruncatePostingIterator.h"
#include "indexlib/index/inverted_index/truncate/TruncatePostingIteratorCreator.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, IndexTermExtender);

IndexTermExtender::IndexTermExtender(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                     const std::shared_ptr<TruncateIndexWriter>& truncateIndexWriter,
                                     const std::shared_ptr<MultiAdaptiveBitmapIndexWriter>& adaptiveBitmapWriter)
    : _indexConfig(indexConfig)
    , _truncateIndexWriter(truncateIndexWriter)
    , _adaptiveBitmapIndexWriter(adaptiveBitmapWriter)
{
    _indexFormatOption.Init(indexConfig);
}

void IndexTermExtender::Init(const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegments,
                             std::vector<std::shared_ptr<IndexOutputSegmentResource>>& indexOutputSegmentResources)

{
    if (_truncateIndexWriter) {
        _truncateIndexWriter->Init(targetSegments);
    }

    if (_adaptiveBitmapIndexWriter) {
        _adaptiveBitmapIndexWriter->Init(indexOutputSegmentResources);
    }
}

std::pair<Status, IndexTermExtender::TermOperation>
IndexTermExtender::ExtendTerm(DictKeyInfo key, const std::shared_ptr<PostingMerger>& postingMerger,
                              SegmentTermInfo::TermIndexMode mode)
{
    bool truncateRemain = true;
    bool adaptiveRemain = true;
    if (_truncateIndexWriter) {
        auto [st, remain] = MakeTruncateTermPosting(key, postingMerger, mode);
        if (!st.IsOK()) {
            RETURN2_IF_STATUS_ERROR(st, TO_DISCARD, "make truncate term posting failed");
        }
        truncateRemain = remain;
    }

    if (_adaptiveBitmapIndexWriter && mode != SegmentTermInfo::TM_BITMAP) {
        adaptiveRemain = MakeAdaptiveBitmapTermPosting(key, postingMerger);
    }

    if (truncateRemain && adaptiveRemain) {
        return {Status::OK(), TO_REMAIN};
    }
    return {Status::OK(), TO_DISCARD};
}

void IndexTermExtender::Destroy()
{
    if (_adaptiveBitmapIndexWriter) {
        _adaptiveBitmapIndexWriter->EndPosting();
    }

    if (_truncateIndexWriter) {
        _truncateIndexWriter->EndPosting();
    }
}

std::pair<Status, bool> IndexTermExtender::MakeTruncateTermPosting(index::DictKeyInfo key,
                                                                   const std::shared_ptr<PostingMerger>& postingMerger,
                                                                   SegmentTermInfo::TermIndexMode mode)
{
    bool remainPosting = true;
    df_t df = postingMerger->GetDocFreq();
    TruncateTriggerInfo triggerInfo(key, df);
    if (_truncateIndexWriter->NeedTruncate(triggerInfo)) {
        if (mode == SegmentTermInfo::TM_NORMAL && _indexConfig->IsBitmapOnlyTerm(key)) {
            // fixbug #258648
            int32_t truncPostingCount = _truncateIndexWriter->GetEstimatePostingCount();
            int32_t truncMetaPostingCount = 0;
            _indexConfig->GetTruncatePostingCount(key, truncMetaPostingCount);
            if (truncPostingCount >= truncMetaPostingCount) {
                // only bitmap && normal term only exist in no truncate posting segment
                remainPosting = false;
            }
            // Do not do truncate, bitmap term will cover
            return {Status::OK(), remainPosting};
        }

        std::shared_ptr<PostingIterator> postingIterator = postingMerger->CreatePostingIterator();
        std::shared_ptr<PostingIterator> postingIteratorClone(postingIterator->Clone());
        auto truncatePostingIterator = std::make_shared<TruncatePostingIterator>(postingIterator, postingIteratorClone);
        const std::string& indexName = _indexConfig->GetIndexName();
        AUTIL_LOG(DEBUG, "Begin generating truncate list for %s:%s", indexName.c_str(), key.ToString().c_str());
        auto st = _truncateIndexWriter->AddPosting(key, truncatePostingIterator, postingMerger->GetDocFreq());
        RETURN2_IF_STATUS_ERROR(st, remainPosting, "add truncate posting failed.");
        AUTIL_LOG(DEBUG, "End generating truncate list for %s:%s", indexName.c_str(), key.ToString().c_str());
    }
    return {Status::OK(), remainPosting};
}

bool IndexTermExtender::MakeAdaptiveBitmapTermPosting(index::DictKeyInfo key,
                                                      const std::shared_ptr<PostingMerger>& postingMerger)
{
    bool isAdaptive = _adaptiveBitmapIndexWriter->NeedAdaptiveBitmap(key, postingMerger);
    bool remainPosting = true;
    if (isAdaptive) {
        std::shared_ptr<PostingIterator> postingIter = postingMerger->CreatePostingIterator();
        assert(postingIter);

        const std::string& indexName = _indexConfig->GetIndexName();
        AUTIL_LOG(DEBUG, "Begin generating adaptive bitmap list for %s:%s", indexName.c_str(), key.ToString().c_str());
        _adaptiveBitmapIndexWriter->AddPosting(key, postingMerger->GetTermPayload(), postingIter);
        AUTIL_LOG(DEBUG, "End generating adaptive bitmap list for %s:%s", indexName.c_str(), key.ToString().c_str());

        if (_indexConfig->GetHighFrequencyTermPostingType() == indexlib::index::hp_bitmap) {
            remainPosting = false;
        }
    }
    return remainPosting;
}

std::shared_ptr<PostingIterator>
IndexTermExtender::CreateCommonPostingIterator(const std::shared_ptr<util::ByteSliceList>& sliceList,
                                               uint8_t compressMode, SegmentTermInfo::TermIndexMode mode)
{
    assert(false);
    return {};
    // PostingFormatOption postingFormatOption = _indexFormatOption.GetPostingFormatOption();
    // if (mode == SegmentTermInfo::TM_BITMAP) {
    //     postingFormatOption = postingFormatOption.GetBitmapPostingFormatOption();
    // }

    // SegmentPosting segPosting(postingFormatOption);
    // segPosting.Init(compressMode, sliceList, 0, 0);

    // return TruncatePostingIteratorCreator::Create(_indexConfig, segPosting,
    //                                                                mode ==
    //                                                                SegmentTermInfo::TM_BITMAP);
}

} // namespace indexlib::index
