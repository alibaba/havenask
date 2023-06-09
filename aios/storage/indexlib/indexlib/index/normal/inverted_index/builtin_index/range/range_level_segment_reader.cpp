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
#include "indexlib/index/normal/inverted_index/builtin_index/range/range_level_segment_reader.h"

#include "indexlib/index/inverted_index/format/dictionary/DictionaryCreator.h"

using namespace std;

using namespace indexlib::file_system;

using namespace indexlib::common;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, RangeLevelSegmentReader);

void RangeLevelSegmentReader::Open(const config::IndexConfigPtr& indexConfig,
                                   const index_base::SegmentData& segmentData,
                                   const NormalIndexSegmentReader* hintReader)
{
    assert(indexConfig);
    mSegmentData = segmentData;
    DirectoryPtr parentDirectory = segmentData.GetIndexDirectory(mParentIndexName, false);
    if (!parentDirectory) {
        IE_LOG(WARN, "index dir [%s] not exist in segment [%d]", mParentIndexName.c_str(), segmentData.GetSegmentId());
        return;
    }

    DirectoryPtr indexDirectory = parentDirectory->GetDirectory(indexConfig->GetIndexName(), false);
    if (!indexDirectory) {
        IE_LOG(WARN, "sub index dir [%s] in dir [%s] not exist in segment [%d]", indexConfig->GetIndexName().c_str(),
               mParentIndexName.c_str(), segmentData.GetSegmentId());
        return;
    }
    NormalIndexSegmentReader::Open(indexConfig, indexDirectory, hintReader);
    mIsHashTypeDict = indexConfig->IsHashTypedDictionary();
}

future_lite::coro::Lazy<index::Result<SegmentPosting>>
RangeLevelSegmentReader::FillOneSegment(dictvalue_t value, autil::mem_pool::Pool* sessionPool,
                                        file_system::ReadOption option) noexcept
{
    SegmentPosting segPosting;
    index::ErrorCode result = co_await GetSegmentPostingAsync(value, segPosting, sessionPool, option);
    if (result == index::ErrorCode::OK) {
        co_return segPosting;
    }
    co_return result;
}

future_lite::coro::Lazy<index::ErrorCode> RangeLevelSegmentReader::FillSegmentPostings(
    const RangeFieldEncoder::Ranges& ranges, const shared_ptr<SegmentPostings>& segmentPostings,
    autil::mem_pool::Pool* sessionPool, file_system::ReadOption option) noexcept

{
    if (mIsHashTypeDict) {
        IE_LOG(ERROR, "range index should use tiered dictionary.");
        co_return index::ErrorCode::UnSupported;
    }
    auto dictReader = std::dynamic_pointer_cast<TieredDictionaryReader>(mDictReader);
    if (!dictReader) {
        assert(false);
        IE_LOG(ERROR, "cast failed, range index should use tiered dictionary reader.");
        co_return index::ErrorCode::UnSupported;
    }
    std::shared_ptr<DictionaryIterator> iter = mDictReader->CreateIterator();
    assert(iter);
    std::vector<future_lite::coro::Lazy<index::Result<SegmentPosting>>> tasks;
    for (size_t i = 0; i < ranges.size(); i++) {
        auto seekEc = co_await iter->SeekAsync((dictkey_t)ranges[i].first, option);
        if (seekEc != index::ErrorCode::OK) {
            co_return seekEc;
        }
        while (iter->HasNext()) {
            index::DictKeyInfo key;
            dictvalue_t value = 0;
            auto nextEc = co_await iter->NextAsync(key, option, value);
            if (nextEc != index::ErrorCode::OK) {
                co_return nextEc;
            }
            if (key.IsNull()) {
                continue;
            }
            if (key.GetKey() > ranges[i].second) {
                break;
            }
            tasks.push_back(FillOneSegment(value, sessionPool, option));
        }
    }
    auto taskResult = co_await future_lite::coro::collectAll(std::move(tasks));
    for (size_t i = 0; i < taskResult.size(); ++i) {
        assert(!taskResult[i].hasError());
        if (taskResult[i].value().Ok()) {
            segmentPostings->AddSegmentPosting(taskResult[i].value().Value());
        } else {
            auto ec = taskResult[i].value().GetErrorCode();
            co_return ec;
        }
    }
    co_return index::ErrorCode::OK;
}

}} // namespace indexlib::index
