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
#include <stdint.h>

#include "indexlib/base/Types.h"
#include "indexlib/index/normal/attribute/accessor/single_value_data_iterator.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(merger, SegmentDirectory);
DECLARE_REFERENCE_CLASS(merger, DocumentDeleter);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace merger {

class ObsoleteDocReclaimer
{
public:
    ObsoleteDocReclaimer(const config::IndexPartitionSchemaPtr& schema,
                         util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~ObsoleteDocReclaimer();

public:
    void Reclaim(const DocumentDeleterPtr& docDeleter, const SegmentDirectoryPtr& segDir);
    void RemoveObsoleteSegments(const SegmentDirectoryPtr& segDir);

private:
    size_t DeleteObsoleteDocs(index::SingleValueDataIterator<uint32_t>& iterator, exdocid_t baseDocid,
                              const DocumentDeleterPtr& docDeleter);

private:
    config::IndexPartitionSchemaPtr mSchema;
    int64_t mCurrentTime;
    util::MetricProviderPtr mMetricProvider;
    IE_DECLARE_METRIC(obsoleteDoc);
    IE_DECLARE_METRIC(obsoleteSegment);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ObsoleteDocReclaimer);
}} // namespace indexlib::merger
