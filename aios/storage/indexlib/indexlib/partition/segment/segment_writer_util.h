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

DECLARE_REFERENCE_CLASS(config, IndexPartitionOptions);
DECLARE_REFERENCE_CLASS(config, KKVIndexConfig);
DECLARE_REFERENCE_CLASS(framework, SegmentMetrics);
DECLARE_REFERENCE_CLASS(util, QuotaControl);

namespace indexlib { namespace partition {

class SegmentWriterUtil
{
public:
    static size_t EstimateInitMemUseForKKVSegmentWriter(config::KKVIndexConfigPtr KKVConfig, uint32_t columnIdx,
                                                        uint32_t totalColumnCount,
                                                        const config::IndexPartitionOptions& options,
                                                        const std::shared_ptr<framework::SegmentMetrics>& metrics,
                                                        const util::QuotaControlPtr& buildMemoryQuotaControler);

private:
    SegmentWriterUtil();
    ~SegmentWriterUtil();

    SegmentWriterUtil(const SegmentWriterUtil&) = delete;
    SegmentWriterUtil& operator=(const SegmentWriterUtil&) = delete;
    SegmentWriterUtil(SegmentWriterUtil&&) = delete;
    SegmentWriterUtil& operator=(SegmentWriterUtil&&) = delete;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::partition
