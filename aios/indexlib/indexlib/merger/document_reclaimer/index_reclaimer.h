#ifndef __INDEXLIB_INDEX_RECLAIMER_H
#define __INDEXLIB_INDEX_RECLAIMER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"
#include "indexlib/merger/document_reclaimer/index_reclaimer_param.h"
#include "indexlib/misc/metric_provider.h"
#include "indexlib/misc/monitor.h"

DECLARE_REFERENCE_CLASS(merger, DocumentDeleter)

IE_NAMESPACE_BEGIN(merger);

class IndexReclaimer
{
public:
    IndexReclaimer(const index_base::PartitionDataPtr& partitionData,
                   const IndexReclaimerParam& reclaimParam,
                   misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    virtual ~IndexReclaimer();
public:
    virtual bool Reclaim(const DocumentDeleterPtr& docDeleter) = 0;

protected:
    index_base::PartitionDataPtr mPartitionData;
    IndexReclaimerParam mReclaimParam;
    index::MultiFieldIndexReaderPtr mIndexReaders;
    misc::MetricProviderPtr mMetricProvider;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexReclaimer);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_INDEX_RECLAIMER_H
