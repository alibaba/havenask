#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_creator.h"
#include "indexlib/merger/document_reclaimer/and_index_reclaimer.h"
#include "indexlib/merger/document_reclaimer/index_field_reclaimer.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, IndexReclaimerCreator);

IndexReclaimerCreator::IndexReclaimerCreator(
        const IndexPartitionSchemaPtr& schema,
        const PartitionDataPtr& partitionData,
        misc::MetricProviderPtr metricProvider)
    : mIndexSchema(schema->GetIndexSchema())
    , mPartitionData(partitionData)
    , mMetricProvider(metricProvider)
{
    mIndexReaders.reset(new MultiFieldIndexReader);
}

IndexReclaimerCreator::~IndexReclaimerCreator() 
{
}

IndexReclaimer* IndexReclaimerCreator::Create(
        const IndexReclaimerParam& param) const
{
    if (!mIndexSchema) {
        IE_LOG(ERROR, "indexSchema not exist");
        return NULL;
    }
    const string op = param.GetReclaimOperator();
    if (op != IndexReclaimerParam::AND_RECLAIM_OPERATOR)
    {
        std::unique_ptr<IndexFieldReclaimer> fieldReclaimer(
            new IndexFieldReclaimer(mPartitionData, param, mMetricProvider));
        const string& name = param.GetReclaimIndex();
        IndexConfigPtr indexConfig = mIndexSchema->GetIndexConfig(name);
        if (!indexConfig)
        {
            IE_LOG(ERROR, "index [%s] not exist", name.c_str());
            return NULL;
        }
        if (!fieldReclaimer->Init(indexConfig, mIndexReaders)) {
            return NULL;
        }
        return fieldReclaimer.release();
    }
    
    std::unique_ptr<AndIndexReclaimer> andIndexReclaimer(
        new AndIndexReclaimer(mPartitionData, param, mMetricProvider));
    if (!andIndexReclaimer->Init(mIndexSchema, mIndexReaders)) {
        return NULL;
    }
    return andIndexReclaimer.release();
}

IE_NAMESPACE_END(merger);

