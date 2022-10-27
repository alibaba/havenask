#include "indexlib/partition/patch_loader.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/patch_iterator_creator.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/single_field_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/modifier/inplace_modifier.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/util/thread_pool.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, PatchLoader);

int64_t PatchLoader::PATCH_WORK_ITEM_COST_SAMPLE_COUNT = 2000;

PatchLoader::PatchLoader(const IndexPartitionSchemaPtr& schema,
                         const OnlineConfig& onlineConfig)
    : mSchema(schema)
    , mOnlineConfig(onlineConfig)
{
}

PatchLoader::~PatchLoader() 
{
    for (size_t i = 0; i < mPatchWorkItems.size(); ++i)
    {
        if (mPatchWorkItems[i])
        {
            delete mPatchWorkItems[i];
            mPatchWorkItems[i] = NULL;
        }
    } 
}

void PatchLoader::Init(const PartitionDataPtr& partitionData,
                       bool isIncConsistentWithRt,
                       const Version& lastLoadVersion,
                       segmentid_t startLoadSegment,
                       bool loadPatchForOpen)
{
    mPartitionData = partitionData;
    int64_t timeBegin = TimeUtility::currentTime();
    mIter = PatchIteratorCreator::Create(mSchema, partitionData,
            isIncConsistentWithRt, lastLoadVersion, startLoadSegment);

    if (loadPatchForOpen && mOnlineConfig.loadPatchThreadNum > 1)
    {
        CreatePatchWorkItems(mIter, mPatchWorkItems);
    }
    int64_t timeAfterCreate = TimeUtility::currentTime();
    IE_LOG(INFO, "load patch, create iterator use [%ld] ms",
           (timeAfterCreate - timeBegin) / 1000);

}

size_t PatchLoader::CalculatePatchLoadExpandSize()
{
    return mIter->GetPatchLoadExpandSize();
};

size_t PatchLoader::SingleThreadLoadPatch(
    const PartitionModifierPtr& modifier)
{
    size_t count = 0;
    AttrFieldValue value;
    mIter->Reserve(value);
    while(mIter->HasNext())
    {
        mIter->Next(value);
        modifier->UpdateField(value);
        ++count;
    }
    return count;
}

void PatchLoader::CreatePatchWorkItems(
    const PatchIteratorPtr& iter,
    vector<AttributePatchWorkItem*>& workItems)
{
    iter->CreateIndependentPatchWorkItems(workItems);
}

bool PatchLoader::InitPatchWorkItems(const PartitionModifierPtr& modifier,
                                     vector<AttributePatchWorkItem*>& workItems) 
{
    const IndexPartitionSchemaPtr& subSchema = 
        mSchema->GetSubIndexPartitionSchema();

    PartitionModifierPtr mainModifier = modifier;
    PartitionModifierPtr subModifier;;
    if (subSchema)
    {
        SubDocModifierPtr subDocModifier = DYNAMIC_POINTER_CAST(SubDocModifier, modifier);
        if (!subDocModifier)
        {
            IE_LOG(ERROR, "modifier cannot be cast to SubDocModifier");
            return false;            
        }
        mainModifier = subDocModifier->GetMainModifier();
        subModifier = subDocModifier->GetSubModifier();
    }
    for (auto& workItem : workItems)
    {
        auto partModifier = workItem->IsSub() ? subModifier : mainModifier;;
        auto inplaceModifier = DYNAMIC_POINTER_CAST(partition::InplaceModifier, partModifier);
        if (!inplaceModifier)
        {
            IE_LOG(ERROR, "modifier cannot be cast to InplaceModifier when initializing [%s]",
                   workItem->GetIdentifier().c_str());
            return false;
        }
        auto deletionMapReader = inplaceModifier->GetDeletionMapReader();
        auto attrModifier = inplaceModifier->GetAttributeModifier();
    
        if (!workItem->Init(deletionMapReader, attrModifier))
        {
            IE_LOG(ERROR, "Init AttributePatchWorkItem[%s] failed",
                   workItem->GetIdentifier().c_str());
            return false;
        }
    }
    return true;
}

size_t PatchLoader::EstimatePatchWorkItemCost(
    vector<index::AttributePatchWorkItem*>& patchWorkItems,
    const ThreadPoolPtr& threadPool)
{
    auto CompareByTypeAndCount = [](const AttributePatchWorkItem* lhs,
                                    const AttributePatchWorkItem* rhs)
    {
        if (lhs->GetItemType() == rhs->GetItemType())
        {
            return lhs->GetPatchItemCount() > rhs->GetPatchItemCount();
        }
        return lhs->GetItemType() > rhs->GetItemType();
    };
    sort(patchWorkItems.begin(), patchWorkItems.end(), CompareByTypeAndCount);
    for (auto& workItem : patchWorkItems)
    {
        workItem->SetProcessLimit(PATCH_WORK_ITEM_COST_SAMPLE_COUNT);
        threadPool->PushWorkItem(workItem);
    }
    threadPool->WaitFinish();

    size_t count = 0;
    for (auto& workItem : patchWorkItems)
    {
        size_t lastProcessCount = workItem->GetLastProcessCount();
        size_t totalPatchCount = workItem->GetPatchItemCount();        
        int64_t lastProcessTimeInMicroSeconds = workItem->GetLastProcessTime();
        size_t estimateTotalProcessTime = (lastProcessCount == 0) ? 0
            : lastProcessTimeInMicroSeconds * (totalPatchCount/lastProcessCount);
        assert(estimateTotalProcessTime - lastProcessTimeInMicroSeconds >=  0);
        workItem->SetCost(estimateTotalProcessTime - lastProcessTimeInMicroSeconds);
        count += lastProcessCount;
    }
    return count;
}

void PatchLoader::SortPatchWorkItemsByCost(
    vector<index::AttributePatchWorkItem*>& patchWorkItems)
{
    auto CompareByCost = [](const AttributePatchWorkItem* lhs,
                            const AttributePatchWorkItem* rhs)
    {
        return lhs->GetCost() > rhs->GetCost();
    };
    sort(patchWorkItems.begin(), patchWorkItems.end(), CompareByCost);
}

size_t PatchLoader::MultiThreadLoadPatch(
    const PartitionModifierPtr& modifier)
{
    if (!InitPatchWorkItems(modifier, mPatchWorkItems))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "init patch work items failed");
    }
    ThreadPoolPtr threadPool(
        new ThreadPool(mOnlineConfig.loadPatchThreadNum, mPatchWorkItems.size()));
    threadPool->Start("indexLoadPatch");
    size_t count =  0;
    count += EstimatePatchWorkItemCost(mPatchWorkItems, threadPool);
    SortPatchWorkItemsByCost(mPatchWorkItems);
    
    for (size_t i = 0; i < mPatchWorkItems.size(); ++i)
    {
        mPatchWorkItems[i]->SetProcessLimit(numeric_limits<size_t>::max());
        threadPool->PushWorkItem(mPatchWorkItems[i]);
    }
    threadPool->WaitFinish();
    threadPool->Stop();

    for (size_t i = 0; i < mPatchWorkItems.size(); ++i)
    {
        count += mPatchWorkItems[i]->GetLastProcessCount();
    }
    return count;
}

void PatchLoader::Load(const Version& patchLoadedVersion,
                       const PartitionModifierPtr& modifier)
{
    assert(mPartitionData);
    if (!NeedLoadPatch(mPartitionData->GetVersion(),
                       patchLoadedVersion))
    {
        return;
    }
    int64_t timeBeginLoad = TimeUtility::currentTime();

    size_t count = 0;
    uint32_t threadCount = mOnlineConfig.loadPatchThreadNum;
    if (mPatchWorkItems.size() == 0
        ||  threadCount == 1)
    {
        count = SingleThreadLoadPatch(modifier);
    }
    else
    {
        IE_LOG(INFO, "enable multi thread load patch, threadNum = [%u]", threadCount);
        count = MultiThreadLoadPatch(modifier);
    }
    int64_t timeEnd = TimeUtility::currentTime();
    IE_LOG(INFO, "load finish, total updated field [%zu], "
           "update use [%ld] ms",
           count, (timeEnd - timeBeginLoad) / 1000);
}

bool PatchLoader::NeedLoadPatch(const Version& currentVersion,
                                const Version& loadedVersion) const
{
    if (!(currentVersion > loadedVersion))
    {
        return false;
    }

    if (HasUpdatableAttribute(mSchema))
    {
        return true;
    }

    IndexPartitionSchemaPtr subSchema = mSchema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        return HasUpdatableAttribute(subSchema);
    }
    return false;
}

// only for test
void PatchLoader::LoadAttributePatch(AttributeReader& attrReader,
                                     const AttributeConfigPtr& attrConfig,
                                     const PartitionDataPtr& partitionData)
{
    assert(attrConfig);
    assert(partitionData);

    SingleFieldPatchIterator patchIter(attrConfig, false);
    patchIter.Init(partitionData, false, INVALID_SEGMENTID);

    AttrFieldValue value;
    patchIter.Reserve(value);
    while(patchIter.HasNext())
    {
        patchIter.Next(value);
        attrReader.UpdateField(value.GetDocId(), 
                               value.Data(), value.GetDataSize());
    }
}

bool PatchLoader::HasUpdatableAttribute(
        const config::IndexPartitionSchemaPtr& schema)
{
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    if (!indexSchema->HasPrimaryKeyIndex())
    {
        return false;
    }

    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (!attrSchema)
    {
        return false;
    }

    bool hasUpdateAttribute = false;
    auto attrConfigs = attrSchema->CreateIterator();
    for (auto iter = attrConfigs->Begin(); iter != attrConfigs->End(); iter++)
    {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->IsAttributeUpdatable())
        {
            hasUpdateAttribute = true;
            break;
        }
    }
    return hasUpdateAttribute;    
}

IE_NAMESPACE_END(partition);

