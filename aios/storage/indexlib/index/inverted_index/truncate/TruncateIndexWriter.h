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

#include "indexlib/base/Status.h"
#include "indexlib/file_system/fslib/IoConfig.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/truncate/TruncateTrigger.h"

namespace indexlib::index {

class TruncateIndexWriter
{
public:
    TruncateIndexWriter() : _estimatePostingCount(0) {}
    virtual ~TruncateIndexWriter() {}

public:
    virtual void Init(const std::vector<std::shared_ptr<indexlibv2::framework::SegmentMeta>>& targetSegmentMetas) = 0;
    virtual bool NeedTruncate(const TruncateTriggerInfo& info) const = 0;
    virtual Status AddPosting(const DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt,
                              df_t docFreq) = 0;
    virtual void EndPosting() = 0;
    virtual int32_t GetEstimatePostingCount() const { return _estimatePostingCount; };
    virtual void SetIOConfig(const file_system::IOConfig& ioConfig) {}
    virtual int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount,
                                      size_t outputSegmentCount) const = 0;

protected:
    mutable int32_t _estimatePostingCount;
};

} // namespace indexlib::index
