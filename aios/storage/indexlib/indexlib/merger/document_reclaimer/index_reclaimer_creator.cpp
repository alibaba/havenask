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
#include "indexlib/merger/document_reclaimer/index_reclaimer_creator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/document_reclaimer/and_index_reclaimer.h"
#include "indexlib/merger/document_reclaimer/index_field_reclaimer.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, IndexReclaimerCreator);

IndexReclaimerCreator::IndexReclaimerCreator(const IndexPartitionSchemaPtr& schema,
                                             const PartitionDataPtr& partitionData,
                                             util::MetricProviderPtr metricProvider)
    : mIndexSchema(schema->GetIndexSchema())
    , mPartitionData(partitionData)
    , mMetricProvider(metricProvider)
{
    mIndexReaders.reset(new index::legacy::MultiFieldIndexReader);
}

IndexReclaimerCreator::~IndexReclaimerCreator() {}

IndexReclaimer* IndexReclaimerCreator::Create(const IndexReclaimerParam& param) const
{
    if (!mIndexSchema) {
        IE_LOG(ERROR, "indexSchema not exist");
        return NULL;
    }
    const string op = param.GetReclaimOperator();
    if (op != IndexReclaimerParam::AND_RECLAIM_OPERATOR) {
        std::unique_ptr<IndexFieldReclaimer> fieldReclaimer(
            new IndexFieldReclaimer(mPartitionData, param, mMetricProvider));
        const string& name = param.GetReclaimIndex();
        IndexConfigPtr indexConfig = mIndexSchema->GetIndexConfig(name);
        if (!indexConfig) {
            IE_LOG(ERROR, "index [%s] not exist", name.c_str());
            return NULL;
        }
        if (!fieldReclaimer->Init(indexConfig, mIndexReaders)) {
            return NULL;
        }
        return fieldReclaimer.release();
    }

    std::unique_ptr<AndIndexReclaimer> andIndexReclaimer(new AndIndexReclaimer(mPartitionData, param, mMetricProvider));
    if (!andIndexReclaimer->Init(mIndexSchema, mIndexReaders)) {
        return NULL;
    }
    return andIndexReclaimer.release();
}
}} // namespace indexlib::merger
