#include "indexlib/file_system/storage_metrics.h"

using namespace std;

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, StorageMetrics);

StorageMetrics::StorageMetrics() 
    : mMetricsItemVec(FSFT_UNKNOWN)
{
}

StorageMetrics::~StorageMetrics() 
{
}

IE_NAMESPACE_END(file_system);

