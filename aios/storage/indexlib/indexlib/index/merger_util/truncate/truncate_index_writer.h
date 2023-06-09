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
#ifndef __INDEXLIB_TRUNCATE_INDEX_WRITER_H
#define __INDEXLIB_TRUNCATE_INDEX_WRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/merger_util/truncate/truncate_common.h"
#include "indexlib/index_base/index_meta/output_segment_merge_info.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class TruncateIndexWriter
{
public:
    TruncateIndexWriter() : mEstimatePostingCount(0) {}
    virtual ~TruncateIndexWriter() {}

public:
    virtual void Init(const index_base::OutputSegmentMergeInfos& outputSegmentMergeInfos) = 0;
    virtual bool NeedTruncate(const TruncateTriggerInfo& info) const = 0;
    virtual void AddPosting(const index::DictKeyInfo& dictKey, const std::shared_ptr<PostingIterator>& postingIt,
                            df_t docFreq = -1) = 0;
    virtual void EndPosting() = 0;
    virtual int32_t GetEstimatePostingCount() { return mEstimatePostingCount; };
    virtual void SetIOConfig(const config::MergeIOConfig& ioConfig) {}
    virtual int64_t EstimateMemoryUse(int64_t maxPostingLen, uint32_t totalDocCount,
                                      size_t outputSegmentCount) const = 0;

protected:
    mutable int32_t mEstimatePostingCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateIndexWriter);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_TRUNCATE_INDEX_WRITER_H
