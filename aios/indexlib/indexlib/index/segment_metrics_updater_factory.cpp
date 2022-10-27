#include "indexlib/index/segment_metrics_updater_factory.h"

using namespace std;
IE_NAMESPACE_USE(plugin);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SegmentMetricsUpdaterFactory);

const string SegmentMetricsUpdaterFactory::SEGMENT_METRICS_UPDATER_FACTORY_SUFFIX = "Factory_MetricsUpdater";

SegmentMetricsUpdaterFactory::SegmentMetricsUpdaterFactory() 
{
}

SegmentMetricsUpdaterFactory::~SegmentMetricsUpdaterFactory() 
{
}

IE_NAMESPACE_END(index);

