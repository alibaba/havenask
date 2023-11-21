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

#include <stddef.h>
#include <vector>

#include "indexlib/config/index_partition_options.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace table {

class TableResource
{
public:
    TableResource();
    virtual ~TableResource();

public:
    virtual bool Init(const std::vector<SegmentMetaPtr>& segmentMetas, const config::IndexPartitionSchemaPtr& schema,
                      const config::IndexPartitionOptions& options) = 0;
    virtual bool ReInit(const std::vector<SegmentMetaPtr>& segmentMetas) = 0;

    virtual size_t EstimateInitMemoryUse(const std::vector<SegmentMetaPtr>& segmentMetas) const = 0;
    virtual size_t GetCurrentMemoryUse() const = 0;

public:
    void SetMetricProvider(const util::MetricProviderPtr& metricProvider) { mMetricProvider = metricProvider; }

    const util::MetricProviderPtr& GetMetricProvider() const { return mMetricProvider; }

private:
    util::MetricProviderPtr mMetricProvider;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableResource);
}} // namespace indexlib::table
