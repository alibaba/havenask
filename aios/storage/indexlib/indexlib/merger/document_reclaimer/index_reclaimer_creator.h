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
#ifndef __INDEXLIB_INDEX_RELCAIMER_CREATOR_H
#define __INDEXLIB_INDEX_RELCAIMER_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, IndexSchema);

namespace indexlib { namespace merger {

class IndexReclaimerCreator
{
public:
    IndexReclaimerCreator(const config::IndexPartitionSchemaPtr& schema,
                          const index_base::PartitionDataPtr& partitionData,
                          util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~IndexReclaimerCreator();

public:
    IndexReclaimer* Create(const IndexReclaimerParam& param) const;

private:
    config::IndexSchemaPtr mIndexSchema;
    index_base::PartitionDataPtr mPartitionData;
    index::legacy::MultiFieldIndexReaderPtr mIndexReaders;
    util::MetricProviderPtr mMetricProvider;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexReclaimerCreator);
}} // namespace indexlib::merger

#endif //__INDEXLIB_INDEX_RELCAIMER_CREATOR_H
