#include "indexlib/index/normal/attribute/accessor/pack_field_patch_iterator.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/index/normal/attribute/accessor/pack_attr_update_doc_size_calculator.h"
#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"
#include "indexlib/index/normal/attribute/accessor/pack_field_patch_work_item.h"
#include "indexlib/index_base/index_meta/version.h"
#include <memory>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PackFieldPatchIterator);

PackFieldPatchIterator::~PackFieldPatchIterator()
{
    while (!mHeap.empty())
    {
        SingleFieldPatchIterator* iter = mHeap.top();
        mHeap.pop();
        assert(iter);
        delete iter;
    }
}

void PackFieldPatchIterator::Init(
        const PartitionDataPtr& partitionData, bool isIncConsistentWithRt,
        const index_base::Version& lastLoadedVersion, segmentid_t startLoadSegment)
{
    const vector<AttributeConfigPtr>& attrConfVec =
        mPackAttrConfig->GetAttributeConfigVec();

    for (size_t i = 0; i < attrConfVec.size(); ++i)
    {
        SingleFieldPatchIterator* singleFieldIter =
            CreateSingleFieldPatchIterator(
                    partitionData, isIncConsistentWithRt,
                    startLoadSegment, attrConfVec[i]);
        if (singleFieldIter)
        {
            mPatchLoadExpandSize += singleFieldIter->GetPatchLoadExpandSize();
            attrid_t attrId = singleFieldIter->GetAttributeId();
            AttrFieldValuePtr attrValue(new AttrFieldValue);
            singleFieldIter->Reserve(*attrValue);
            if ((size_t)attrId >= mPatchValues.size())
            {
                mPatchValues.resize(attrId + 1);
            }
            mPatchValues[attrId] = attrValue;
            mHeap.push(singleFieldIter);
            mPatchItemCount += singleFieldIter->GetPatchItemCount();
        }
    }

    Version diffVersion = partitionData->GetVersion() - lastLoadedVersion;
    mPatchLoadExpandSize += PackAttrUpdateDocSizeCalculator::EsitmateOnePackAttributeUpdateDocSize(
            mPackAttrConfig, partitionData, diffVersion);
    mDocId = GetNextDocId();
}

void PackFieldPatchIterator::CreateIndependentPatchWorkItems(
    vector<AttributePatchWorkItem*>& workItems)
{
    stringstream ss;
    ss << mPackAttrConfig->GetAttrName() << "_";
    if (mIsSub)
    {
        ss << "sub";
    }
    else
    {
        ss << "main";
    }    
    AttributePatchWorkItem* item = new PackFieldPatchWorkItem(
        ss.str(), mPatchItemCount, mIsSub, this);
    workItems.push_back(item);
}

SingleFieldPatchIterator*
PackFieldPatchIterator::CreateSingleFieldPatchIterator(
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

void PackFieldPatchIterator::Reserve(AttrFieldValue& value)
{
    PackAttributeFormatter::PackAttributeFields patchFields;
    for (size_t i = 0; i < mPatchValues.size(); i++)
    {
        if (!mPatchValues[i])
        {
            continue;
        }

        uint32_t buffSize = mPatchValues[i]->BufferLength();
        if (buffSize > 0)
        {
            patchFields.push_back(make_pair(
                            INVALID_ATTRID, ConstString(
                                    (const char*)mPatchValues[i]->Data(), buffSize)));
        }
    }
    uint32_t maxLength = 
        PackAttributeFormatter::GetEncodePatchValueLen(patchFields);
    value.ReserveBuffer(maxLength);
    IE_LOG(INFO, "Reserve Buffer size[%lu] for AttrFieldValue", 
           value.BufferLength());
}

void PackFieldPatchIterator::Next(AttrFieldValue& value)
{
    value.SetIsPackAttr(true);
    value.SetIsSubDocId(mIsSub);

    if (!HasNext())
    {
        value.SetDocId(INVALID_DOCID);
        value.SetPackAttrId(INVALID_PACK_ATTRID);
        return;
    }
    
    PackAttributeFormatter::PackAttributeFields patchFields;
    assert(!mHeap.empty());
    do
    {
        unique_ptr<SingleFieldPatchIterator> iter(mHeap.top());
        assert(iter);
        mHeap.pop();
        attrid_t attrId = iter->GetAttributeId();
        assert(mPatchValues[attrId]);
        iter->Next(*mPatchValues[attrId]);
        patchFields.push_back(make_pair(attrId, 
                        ConstString((const char*)mPatchValues[attrId]->Data(),
                                mPatchValues[attrId]->GetDataSize())));
        if (iter->HasNext())
        {
            mHeap.push(iter.release());
        }
        else
        {
            iter.reset();
        }
    }
    while(!mHeap.empty() && mDocId == mHeap.top()->GetCurrentDocId());

    size_t valueLen = PackAttributeFormatter::EncodePatchValues(
        patchFields, value.Data(), value.BufferLength());
    if (valueLen == 0)
    {
        value.SetDocId(INVALID_DOCID);
        value.SetPackAttrId(INVALID_PACK_ATTRID);
        IE_LOG(ERROR, "encode patch value for pack attribute [%s] failed.",
               mPackAttrConfig->GetAttrName().c_str());
        return;
    }
    
    value.SetDocId(mDocId);
    value.SetPackAttrId(mPackAttrConfig->GetPackAttrId());
    value.SetDataSize(valueLen);
    mDocId = GetNextDocId();
}


IE_NAMESPACE_END(index);

