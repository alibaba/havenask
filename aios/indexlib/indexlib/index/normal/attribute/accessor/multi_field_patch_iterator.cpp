#include "indexlib/index/normal/attribute/accessor/multi_field_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/single_field_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/pack_field_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiFieldPatchIterator);

MultiFieldPatchIterator::MultiFieldPatchIterator(
        const IndexPartitionSchemaPtr& schema) 
    : mSchema(schema)
    , mIsSub(false)
    , mPatchLoadExpandSize(0)
{
}

MultiFieldPatchIterator::~MultiFieldPatchIterator() 
{
    while (!mHeap.empty())
    {
        AttributePatchIterator* iter = mHeap.top();
        mHeap.pop();
        delete iter;
    }
}

void MultiFieldPatchIterator::Reserve(AttrFieldValue& value)
{
    vector<AttributePatchIterator*> tmp;
    tmp.reserve(mHeap.size());
    while (!mHeap.empty())
    {
        AttributePatchIterator* iter = mHeap.top();
        mHeap.pop();
        tmp.push_back(iter);
        assert(iter);
        iter->Reserve(value);
    }
    for (size_t i = 0; i < tmp.size(); i++)
    {
        mHeap.push(tmp[i]);
    }

    IE_LOG(INFO, "Reserve Buffer size[%lu] for AttrFieldValue", 
           value.BufferLength());
}

void MultiFieldPatchIterator::Init(
        const PartitionDataPtr& partitionData,
        bool isIncConsistentWithRt,
        const Version& lastLoadVersion,
        segmentid_t startLoadSegment,
        bool isSub)
{
    mIsSub = isSub;
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema)
    {
        return;
    }
    auto attrConfigs = attrSchema->CreateIterator();
    auto iter = attrConfigs->Begin();
    for (; iter != attrConfigs->End(); iter++)
    {
        const AttributeConfigPtr& attrConfig = *iter;
        if (attrConfig->GetPackAttributeConfig() != NULL)
        {
            continue;
        }
        unique_ptr<AttributePatchIterator> attrPatchIter( 
            CreateSingleFieldPatchIterator(partitionData, isIncConsistentWithRt,
                    startLoadSegment, attrConfig));

        if (attrPatchIter)
        {
            mPatchLoadExpandSize += attrPatchIter->GetPatchLoadExpandSize();
            mHeap.push(attrPatchIter.release());
        }
    }

    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++)
    {
        auto& packAttrConfig = *packIter;
        unique_ptr<AttributePatchIterator> attrPatchIter( 
            CreatePackFieldPatchIterator(partitionData, isIncConsistentWithRt, lastLoadVersion,
                    startLoadSegment, packAttrConfig));
        if (attrPatchIter)
        {
            mPatchLoadExpandSize += attrPatchIter->GetPatchLoadExpandSize();
            mHeap.push(attrPatchIter.release());
        }
    }
}

void MultiFieldPatchIterator::CreateIndependentPatchWorkItems(
    vector<AttributePatchWorkItem*>& workItems)
{
    vector<AttributePatchIterator*> tmp;
    tmp.reserve(mHeap.size());
    while (!mHeap.empty())
    {
        AttributePatchIterator* iter = mHeap.top();
        mHeap.pop();
        tmp.push_back(iter);
        assert(iter);
        iter->CreateIndependentPatchWorkItems(workItems);
    }
    
    for (size_t i = 0; i < tmp.size(); i++)
    {
        mHeap.push(tmp[i]);
    }
}

AttributePatchIterator* 
MultiFieldPatchIterator::CreateSingleFieldPatchIterator(
    const PartitionDataPtr& partitionData, 
    bool isIncConsistentWithRt,
    segmentid_t startLoadSegment,
    const AttributeConfigPtr& attrConf)
{
    if (!attrConf->IsAttributeUpdatable())
    {
        return NULL;
    }
    unique_ptr<SingleFieldPatchIterator> singleFieldIter(
            new SingleFieldPatchIterator(attrConf, mIsSub));
    singleFieldIter->Init(partitionData, isIncConsistentWithRt, startLoadSegment);
    if (!singleFieldIter->HasNext())
    {
        return NULL;
    }
    return singleFieldIter.release();
}

AttributePatchIterator* 
MultiFieldPatchIterator::CreatePackFieldPatchIterator(
    const PartitionDataPtr& partitionData, 
    bool isIncConsistentWithRt,
    const Version& lastLoadVersion,
    segmentid_t startLoadSegment,
    const PackAttributeConfigPtr& packAttrConf)
{
    unique_ptr<PackFieldPatchIterator> packFieldIter(
            new PackFieldPatchIterator(packAttrConf, mIsSub));
    packFieldIter->Init(partitionData, isIncConsistentWithRt, lastLoadVersion, startLoadSegment);
    if (!packFieldIter->HasNext())
    {
        return NULL;
    }
    return packFieldIter.release();
}

void MultiFieldPatchIterator::Next(AttrFieldValue& value)
{
    if (!HasNext())
    {   
        value.SetDocId(INVALID_DOCID);
        value.SetFieldId(INVALID_FIELDID);
        return;
    }
    unique_ptr<AttributePatchIterator> patchIter(mHeap.top());
    mHeap.pop();
    patchIter->Next(value);
    if (patchIter->HasNext())
    {
        mHeap.push(patchIter.release());
    }
    else
    {
        patchIter.reset();
    }
}

IE_NAMESPACE_END(index);

