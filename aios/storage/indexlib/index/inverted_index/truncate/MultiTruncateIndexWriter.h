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

#include "indexlib/index/inverted_index/truncate/ITruncateWriterScheduler.h"
#include "indexlib/index/inverted_index/truncate/TruncateIndexWriter.h"

namespace indexlib::index {

class MultiTruncateIndexWriter : public TruncateIndexWriter
{
public:
    MultiTruncateIndexWriter();
    ~MultiTruncateIndexWriter() = default;

public:
    void Init(const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& segmentMetas) override;
    bool NeedTruncate(const TruncateTriggerInfo& info) const override;
    Status AddPosting(const DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt,
                      df_t docFreq) override;
    void EndPosting() override;
    int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount, size_t outputSegmentCount) const override;
    void AddIndexWriter(const std::shared_ptr<TruncateIndexWriter>& indexWriter);

private:
    void SetTruncateThreadCount(uint32_t threadCount);

private:
    std::vector<std::shared_ptr<TruncateIndexWriter>> _truncateIndexWriters;
    std::unique_ptr<ITruncateWriterScheduler> _scheduler;
    static constexpr uint32_t DEFAULT_TRUNC_THREAD_COUNT = 2;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
