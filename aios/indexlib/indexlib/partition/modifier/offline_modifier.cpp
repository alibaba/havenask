#include "indexlib/partition/modifier/offline_modifier.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/partition_data.h"

using namespace std;
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OfflineModifier);

void OfflineModifier::Init(const PartitionDataPtr& partitionData,
                           const BuildResourceMetricsPtr& buildResourceMetrics)
{
    PatchModifier::Init(partitionData, buildResourceMetrics);
    mDeletionMapReader = partitionData->GetDeletionMapReader();
    assert(mDeletionMapReader);
}

bool OfflineModifier::RemoveDocument(docid_t docId)
{
    if (!PatchModifier::RemoveDocument(docId))
    {
        return false;
    }
    mDeletionMapReader->Delete(docId);
    return true;
}

IE_NAMESPACE_END(partition);

