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
#include "indexlib/partition/segment/segment_writer_util.h"

#include "indexlib/config/kkv_index_config.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/index/kkv/prefix_key_resource_assigner.h"

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, SegmentWriterUtil);

size_t SegmentWriterUtil::EstimateInitMemUseForKKVSegmentWriter(
    config::KKVIndexConfigPtr KKVConfig, uint32_t columnIdx, uint32_t totalColumnCount,
    const config::IndexPartitionOptions& options, const std::shared_ptr<framework::SegmentMetrics>& metrics,
    const util::QuotaControlPtr& buildMemoryQuotaControler)
{
    index::PrefixKeyResourceAssigner assigner(KKVConfig, columnIdx, totalColumnCount);
    assigner.Init(buildMemoryQuotaControler, options);
    return assigner.Assign(metrics);
}

}} // namespace indexlib::partition
