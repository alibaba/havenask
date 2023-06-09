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
#include "indexlib/index/inverted_index/truncate/MultiTruncateIndexWriter.h"

#include "autil/StringUtil.h"
#include "indexlib/base/Status.h"
#include "indexlib/index/inverted_index/truncate/MultiTruncateWriterScheduler.h"
#include "indexlib/index/inverted_index/truncate/SimpleTruncateWriterScheduler.h"
#include "indexlib/index/inverted_index/truncate/TruncateWorkItem.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, MultiTruncateIndexWriter);

MultiTruncateIndexWriter::MultiTruncateIndexWriter()
{
    uint32_t threadCount = DEFAULT_TRUNC_THREAD_COUNT;
    auto threadCountEnv = getenv("TRUNCATE_THREAD_COUNT");
    if (threadCountEnv) {
        autil::StringUtil::fromString(threadCountEnv, threadCount);
    }
    SetTruncateThreadCount(threadCount);
}

void MultiTruncateIndexWriter::Init(
    const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& segmentMetas)
{
    for (size_t i = 0; i < _truncateIndexWriters.size(); ++i) {
        _truncateIndexWriters[i]->Init(segmentMetas);
    }
}

Status MultiTruncateIndexWriter::AddPosting(const DictKeyInfo& dictKey,
                                            const std::shared_ptr<PostingIterator>& postingIt, df_t docFreq)
{
    assert(postingIt);
    TruncateTriggerInfo info(dictKey, docFreq);

    for (size_t i = 0; i < _truncateIndexWriters.size(); ++i) {
        if (_truncateIndexWriters[i]->NeedTruncate(info)) {
            std::shared_ptr<PostingIterator> it(postingIt->Clone());
            auto workItem = new TruncateWorkItem(dictKey, it, _truncateIndexWriters[i]);
            auto st = _scheduler->PushWorkItem(workItem);
            RETURN_IF_STATUS_ERROR(st, "push truncate work item to scheduler failed.");
        }
    }
    auto st = _scheduler->WaitFinished();
    RETURN_IF_STATUS_ERROR(st, "wait scheduler finished failed.");
    return Status::OK();
}

bool MultiTruncateIndexWriter::NeedTruncate(const TruncateTriggerInfo& info) const
{
    _estimatePostingCount = 0;
    bool needTruncate = false;
    for (size_t i = 0; i < _truncateIndexWriters.size(); ++i) {
        if (_truncateIndexWriters[i]->NeedTruncate(info)) {
            needTruncate = true;
            ++_estimatePostingCount;
        }
    }
    return needTruncate;
}

void MultiTruncateIndexWriter::AddIndexWriter(const std::shared_ptr<TruncateIndexWriter>& indexWriter)
{
    _truncateIndexWriters.push_back(indexWriter);
}

void MultiTruncateIndexWriter::EndPosting()
{
    for (size_t i = 0; i < _truncateIndexWriters.size(); ++i) {
        _truncateIndexWriters[i]->EndPosting();
    }
    [[maybe_unused]] auto st = _scheduler->WaitFinished();
    _truncateIndexWriters.clear();
    AUTIL_LOG(DEBUG, "End Posting");
}

void MultiTruncateIndexWriter::SetTruncateThreadCount(uint32_t threadCount)
{
    if (threadCount == 0 || threadCount > 512) {
        AUTIL_LOG(WARN, "invalid truncate thread count [%u], use default [%u]", threadCount,
                  DEFAULT_TRUNC_THREAD_COUNT);
        threadCount = DEFAULT_TRUNC_THREAD_COUNT;
    }
    AUTIL_LOG(INFO, "merge truncate index use %d thread", threadCount);
    if (threadCount > 1) {
        AUTIL_LOG(INFO, "init multi thread truncate scheduler!");
        _scheduler.reset(new MultiTruncateWriterScheduler(threadCount));
    } else {
        AUTIL_LOG(INFO, "init simple truncate scheduler!");
        _scheduler.reset(new SimpleTruncateWriterScheduler());
    }
}

int64_t MultiTruncateIndexWriter::EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount,
                                                    size_t outputSegmentCount) const
{
    std::vector<int64_t> memUseVec(_truncateIndexWriters.size(), 0);
    for (size_t i = 0; i < _truncateIndexWriters.size(); ++i) {
        memUseVec[i] = _truncateIndexWriters[i]->EstimateMemoryUse(maxPostingLen, totalDocCount, outputSegmentCount);
    }
    std::sort(memUseVec.begin(), memUseVec.end());
    std::reverse(memUseVec.begin(), memUseVec.end());
    uint32_t threadCount = 1;
    auto multiScheduler = dynamic_cast<MultiTruncateWriterScheduler*>(_scheduler.get());
    if (multiScheduler) {
        threadCount = multiScheduler->GetThreadCount();
    }

    int64_t memUse = 0;
    for (uint32_t i = 0; i < memUseVec.size() && i < threadCount; ++i) {
        memUse += memUseVec[i];
    }
    return memUse;
}

} // namespace indexlib::index
