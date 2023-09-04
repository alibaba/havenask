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
#include "indexlib/index/merger_util/truncate/multi_truncate_index_writer.h"

#include "autil/StringUtil.h"
#include "indexlib/index/merger_util/truncate/multi_truncate_writer_scheduler.h"
#include "indexlib/index/merger_util/truncate/simple_truncate_writer_scheduler.h"
#include "indexlib/index/merger_util/truncate/trunc_work_item.h"

using namespace std;
using namespace autil;

using namespace indexlib::common;
namespace indexlib::index::legacy {
IE_LOG_SETUP(index, MultiTruncateIndexWriter);

MultiTruncateIndexWriter::MultiTruncateIndexWriter()
{
    uint32_t threadCount = DEFAULT_TRUNC_THREAD_COUNT;
    threadCount = autil::EnvUtil::getEnv("TRUNCATE_THREAD_COUNT", threadCount);
    SetTruncateThreadCount(threadCount);
}

MultiTruncateIndexWriter::~MultiTruncateIndexWriter()
{
    mScheduler.reset();
    mIndexWriters.clear();
}

void MultiTruncateIndexWriter::Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos)
{
    for (size_t i = 0; i < mIndexWriters.size(); i++) {
        mIndexWriters[i]->Init(outputSegmentMergeInfos);
    }
}

void MultiTruncateIndexWriter::AddPosting(const index::DictKeyInfo& dictKey,
                                          const std::shared_ptr<PostingIterator>& postingIt, df_t docFreq)
{
    assert(postingIt);
    TruncateTriggerInfo info(dictKey, docFreq);
    size_t indexWriterCount = mIndexWriters.size();

    for (size_t i = 0; i < indexWriterCount; i++) {
        if (mIndexWriters[i]->NeedTruncate(info)) {
            std::shared_ptr<PostingIterator> it(postingIt->Clone());
            TruncWorkItem* workItem = new TruncWorkItem(dictKey, it, mIndexWriters[i]);
            mScheduler->PushWorkItem(workItem);
        }
    }
    mScheduler->WaitFinished();
}

bool MultiTruncateIndexWriter::NeedTruncate(const TruncateTriggerInfo& info) const
{
    mEstimatePostingCount = 0;
    bool ret = false;
    for (size_t i = 0; i < mIndexWriters.size(); i++) {
        if (mIndexWriters[i]->NeedTruncate(info)) {
            ++mEstimatePostingCount;
            ret = true;
        }
    }
    return ret;
}

void MultiTruncateIndexWriter::AddIndexWriter(const TruncateIndexWriterPtr& indexWriter)
{
    mIndexWriters.push_back(indexWriter);
}

void MultiTruncateIndexWriter::EndPosting()
{
    for (size_t i = 0; i < mIndexWriters.size(); i++) {
        mIndexWriters[i]->EndPosting();
    }
    mScheduler->WaitFinished();
    mIndexWriters.clear();
    IE_LOG(DEBUG, "End Posting");
}

size_t MultiTruncateIndexWriter::GetInternalWriterCount() const { return mIndexWriters.size(); }

void MultiTruncateIndexWriter::SetTruncateThreadCount(uint32_t threadCount)
{
    if (threadCount == 0 || threadCount > 512) {
        IE_LOG(WARN, "invalid truncate thread count [%u], use default [%u]", threadCount, DEFAULT_TRUNC_THREAD_COUNT);
        threadCount = DEFAULT_TRUNC_THREAD_COUNT;
    }

    IE_LOG(INFO, "merge truncate index use %d thread", threadCount);
    if (threadCount > 1) {
        IE_LOG(INFO, "init multi thread truncate scheduler!");
        mScheduler.reset(new MultiTruncateWriterScheduler(threadCount));
    } else {
        IE_LOG(INFO, "init simple truncate scheduler!");
        mScheduler.reset(new SimpleTruncateWriterScheduler());
    }
}

TruncateWriterSchedulerPtr MultiTruncateIndexWriter::getTruncateWriterScheduler() const { return mScheduler; }

int64_t MultiTruncateIndexWriter::EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount,
                                                    size_t outputSegmentCount) const
{
    vector<int64_t> memUseVec(mIndexWriters.size(), 0);
    for (size_t i = 0; i < mIndexWriters.size(); i++) {
        memUseVec[i] = mIndexWriters[i]->EstimateMemoryUse(maxPostingLen, totalDocCount, outputSegmentCount);
    }
    sort(memUseVec.begin(), memUseVec.end());
    reverse(memUseVec.begin(), memUseVec.end());
    uint32_t threadCount = 1;
    MultiTruncateWriterScheduler* multiScheduler = dynamic_cast<MultiTruncateWriterScheduler*>(mScheduler.get());
    if (multiScheduler) {
        threadCount = multiScheduler->GetThreadCount();
    }

    int64_t memUse = 0;
    for (uint32_t i = 0; i < memUseVec.size() && i < threadCount; i++) {
        memUse += memUseVec[i];
    }
    return memUse;
}
} // namespace indexlib::index::legacy
