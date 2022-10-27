#ifndef __INDEXLIB_INDEX_RELCAIMER_CREATOR_H
#define __INDEXLIB_INDEX_RELCAIMER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(merger);

class IndexReclaimerCreator
{
public:
    IndexReclaimerCreator(const config::IndexPartitionSchemaPtr& schema,
                          const index_base::PartitionDataPtr& partitionData,
                          misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~IndexReclaimerCreator();
public:
    IndexReclaimer* Create(const IndexReclaimerParam& param) const;
private:
    config::IndexSchemaPtr mIndexSchema;
    index_base::PartitionDataPtr mPartitionData;
    index::MultiFieldIndexReaderPtr mIndexReaders;
    misc::MetricProviderPtr mMetricProvider;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexReclaimerCreator);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INDEX_RELCAIMER_CREATOR_H
