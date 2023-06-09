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
#ifndef __INDEXLIB_MULTI_TRUNCATE_INDEX_WRITER_H
#define __INDEXLIB_MULTI_TRUNCATE_INDEX_WRITER_H

#include <memory>

#include "autil/OutputOrderedThreadPool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer.h"
#include "indexlib/index/merger_util/truncate/truncate_writer_scheduler.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class MultiTruncateIndexWriter : public TruncateIndexWriter
{
public:
    typedef std::vector<TruncateIndexWriterPtr> IndexWriterList;

public:
    MultiTruncateIndexWriter();
    ~MultiTruncateIndexWriter();

public:
    void Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos) override;
    bool NeedTruncate(const TruncateTriggerInfo& info) const override;
    void AddPosting(const index::DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt,
                    df_t docFreq = -1) override;
    void EndPosting() override;
    void SetTruncateThreadCount(uint32_t threadCount);
    size_t GetInternalWriterCount() const;
    void AddIndexWriter(const TruncateIndexWriterPtr& indexWriter);

    int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount, size_t outputSegmentCount) const override;

public:
    // for test
    TruncateWriterSchedulerPtr getTruncateWriterScheduler() const;

private:
    void CreateTruncateWriteScheduler(bool syncTrunc);

private:
    IndexWriterList mIndexWriters;
    TruncateWriterSchedulerPtr mScheduler;
    static const uint32_t DEFAULT_TRUNC_THREAD_COUNT = 2;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiTruncateIndexWriter);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_MULTI_TRUNCATE_INDEX_WRITER_H
