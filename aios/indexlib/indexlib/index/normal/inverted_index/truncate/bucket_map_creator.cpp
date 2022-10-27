#include "indexlib/index/normal/inverted_index/truncate/bucket_map_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/evaluator_creator.h"
#include "indexlib/util/class_typed_factory.h"
#include "indexlib/index/normal/inverted_index/truncate/sort_work_item.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BucketMapCreator);

BucketMapCreator::BucketMapCreator()
{
}

BucketMapCreator::~BucketMapCreator()
{
}

BucketMaps BucketMapCreator::CreateBucketMaps(
        const config::TruncateOptionConfigPtr& truncOptionConfig,
        TruncateAttributeReaderCreator *attrReaderCreator,
        int64_t maxMemUse)
{
    const TruncateProfileVec& truncateProfiles = truncOptionConfig->GetTruncateProfiles();
    assert(truncateProfiles.size() > 0);
    TruncateProfileVec needSortTruncateProfiles;
    for (size_t i = 0; i < truncateProfiles.size(); i++)
    {
        const TruncateProfilePtr &profile = truncateProfiles[i];
        string strategyName = profile->mTruncateStrategyName;
        TruncateStrategyPtr truncStrategy = truncOptionConfig->GetTruncateStrategy(strategyName);
        if (NeedCreateBucketMap(*profile, *truncStrategy, attrReaderCreator))
        {
            needSortTruncateProfiles.push_back(profile);
        }
    }

    BucketMaps bucketMaps;
    if (needSortTruncateProfiles.empty())
    {
        return bucketMaps;
    }

    const ReclaimMapPtr &reclaimMap = attrReaderCreator->GetReclaimMap();
    int64_t reclaimMapMemUse = reclaimMap->EstimateMemoryUse();
    IE_LOG(INFO, "reclaim map memory use: %f MB", (double)reclaimMapMemUse/1024/1024);

    maxMemUse -= reclaimMapMemUse;

    ResourceControlThreadPoolPtr threadPool = CreateBucketMapThreadPool(
            needSortTruncateProfiles.size(),
            reclaimMap->GetNewDocCount(), maxMemUse);
    vector<ResourceControlWorkItemPtr> workItems;

    int64_t before = autil::TimeUtility::currentTimeInSeconds();
    TruncateProfileVec::const_iterator it = needSortTruncateProfiles.begin();
    for (; it != needSortTruncateProfiles.end(); ++it)
    {
        string strategyName = (*it)->mTruncateStrategyName;
        TruncateStrategyPtr truncStrategy = truncOptionConfig->GetTruncateStrategy(strategyName);
        IE_LOG(INFO, "create bucket map for truncate profile %s",
               (*it)->mTruncateProfileName.c_str());
        BucketMapPtr bucketMap(new BucketMap);
        if (!bucketMap->Init(reclaimMap->GetNewDocCount()))
        {
            IE_LOG(ERROR, "bucket map init error for truncate profile %s NewDocCount(%u)",
                   (*it)->mTruncateProfileName.c_str(), reclaimMap->GetNewDocCount());
            continue;
        }
        bucketMaps.insert(make_pair((*it)->mTruncateProfileName, bucketMap));
        SortWorkItem *workItem = new SortWorkItem((**it), reclaimMap->GetNewDocCount(),
                attrReaderCreator, bucketMap);
        workItems.push_back(ResourceControlWorkItemPtr(workItem));
    }
    threadPool->Init(workItems);
    threadPool->Run("indexBucketMap");
    int64_t after = autil::TimeUtility::currentTimeInSeconds();
    IE_LOG(INFO, "Init bucket map use %ld seconds", (after - before));
    return bucketMaps;
}

bool BucketMapCreator::NeedCreateBucketMap(
        const TruncateProfile& truncateProfile, 
        const TruncateStrategy& truncateStrategy,
        TruncateAttributeReaderCreator *attrReaderCreator)
{
    if (!truncateProfile.HasSort())
    {
        return false;
    }
    
    if (!truncateStrategy.HasLimit())
    {
        return false;
    }

    SortParams sortParams = truncateProfile.mSortParams;
    const AttributeSchemaPtr &attrSchema = attrReaderCreator->GetAttributeSchema();
    for (size_t i = 0; i < sortParams.size(); ++i)
    {
        string sortField = sortParams[i].GetSortField();
        if (!attrSchema->GetAttributeConfig(sortField))
        {
            return false;
        }
    }
    return true;
}

ResourceControlThreadPoolPtr BucketMapCreator::CreateBucketMapThreadPool(
        uint32_t truncateProfileSize,
        uint32_t totalDocCount, int64_t maxMemUse)
{
    int64_t bucketMapSize = sizeof(uint32_t) * totalDocCount;
    bucketMapSize *= truncateProfileSize;
    IE_LOG(INFO, "bucket map memory use: %f MB", (double)bucketMapSize/1024/1024);
    
    maxMemUse -= bucketMapSize;

    int64_t memUsePerSortItem = SortWorkItem::EstimateMemoryUse(totalDocCount);
    if (memUsePerSortItem > maxMemUse)
    {
        IE_LOG(WARN, "leftMemory[%ld] is less than memUsePerSortItem[%ld], adjust to it",
               maxMemUse, memUsePerSortItem);
        maxMemUse = memUsePerSortItem;
    }
    uint32_t threadNum = memUsePerSortItem == 0 ? truncateProfileSize : maxMemUse / memUsePerSortItem;
    threadNum = std::min(threadNum, truncateProfileSize);
    IE_LOG(INFO, "thread num for MultiThreadBucketMapScheduler: [%u].", threadNum);
    return ResourceControlThreadPoolPtr(
            new ResourceControlThreadPool(threadNum, maxMemUse));
}

IE_NAMESPACE_END(index);

