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

#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(config, IndexSchema);

namespace indexlib { namespace merger {

class AndIndexReclaimer : public IndexReclaimer
{
public:
    AndIndexReclaimer(const index_base::PartitionDataPtr& partitionData, const IndexReclaimerParam& reclaimParam,
                      util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~AndIndexReclaimer();

public:
    bool Reclaim(const DocumentDeleterPtr& docDeleter) override;
    bool Init(const config::IndexSchemaPtr& indexSchema, const index::legacy::MultiFieldIndexReaderPtr& indexReaders);

private:
    IE_DECLARE_METRIC(AndReclaim);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AndIndexReclaimer);
}} // namespace indexlib::merger
