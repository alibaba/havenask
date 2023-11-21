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

#include "indexlib/common_define.h"
#include "indexlib/config/summary_group_config.h"
#include "indexlib/index/normal/accessor/group_field_data_merger.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {
// using SummaryMerger = GroupFieldDataMerger;

class SummaryMerger : public GroupFieldDataMerger
{
public:
    SummaryMerger(const config::SummaryGroupConfigPtr& summaryGroupConfig)
        : GroupFieldDataMerger(summaryGroupConfig != nullptr
                                   ? summaryGroupConfig->GetSummaryGroupDataParam().GetFileCompressConfig()
                                   : std::shared_ptr<config::FileCompressConfig>(),
                               SUMMARY_OFFSET_FILE_NAME, SUMMARY_DATA_FILE_NAME)
        , mSummaryGroupConfig(summaryGroupConfig)
    {
    }

    virtual ~SummaryMerger() {}

protected:
    config::SummaryGroupConfigPtr mSummaryGroupConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SummaryMerger);
}} // namespace indexlib::index
