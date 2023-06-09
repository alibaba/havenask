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
#ifndef __INDEXLIB_DATE_INDEX_WRITER_H
#define __INDEXLIB_DATE_INDEX_WRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/config/DateLevelFormat.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_writer.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class TimeRangeInfo;
DEFINE_SHARED_PTR(TimeRangeInfo);

class DateIndexWriter : public index::NormalIndexWriter
{
public:
    DateIndexWriter(size_t lastSegmentDistinctTermCount, const config::IndexPartitionOptions& options);
    ~DateIndexWriter();

public:
    void Init(const config::IndexConfigPtr& indexConfig, util::BuildResourceMetrics* buildResourceMetrics) override;

    void AddField(const document::Field* field) override;
    void Dump(const file_system::DirectoryPtr& dir, autil::mem_pool::PoolBase* dumpPool) override;
    index::IndexSegmentReaderPtr CreateInMemReader() override;

private:
    config::DateLevelFormat mFormat;
    TimeRangeInfoPtr mTimeRangeInfo;
    uint64_t mGranularityLevel;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateIndexWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_DATE_INDEX_WRITER_H
