#include "indexlib/merger/document_reclaimer/index_reclaimer.h"

using namespace std;
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, IndexReclaimer);

IndexReclaimer::IndexReclaimer(
    const index_base::PartitionDataPtr& partitionData,
    const IndexReclaimerParam& reclaimParam,
    misc::MetricProviderPtr metricProvider)
    : mPartitionData(partitionData)
    , mReclaimParam(reclaimParam)
    , mMetricProvider(metricProvider)
{
    mIndexReaders.reset(new MultiFieldIndexReader);
}

IndexReclaimer::~IndexReclaimer() 
{
}

IE_NAMESPACE_END(merger);

