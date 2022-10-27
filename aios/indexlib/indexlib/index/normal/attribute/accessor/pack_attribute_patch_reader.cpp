#include <memory>
#include "indexlib/index/normal/attribute/accessor/pack_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_patch_iterator_creator.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PackAttributePatchReader);

PackAttributePatchReader::~PackAttributePatchReader()
{
    while (!mHeap.empty())
    {
        PatchReaderItem readerItem = mHeap.top();
        mHeap.pop();
        assert(readerItem.first);
        delete readerItem.first;
    }
}

void PackAttributePatchReader::Init(
    const PartitionDataPtr& partitionData, segmentid_t segmentId)
{
    const vector<AttributeConfigPtr>& attrConfVec =
        mPackAttrConfig->GetAttributeConfigVec();

    for (size_t i = 0; i < attrConfVec.size(); ++i)
    {
        unique_ptr<AttributePatchReader> attrPatchReader(CreateSubAttributePatchReader(
                        attrConfVec[i], partitionData, segmentId));
        if (attrPatchReader)
        {
            attrid_t attrId = attrConfVec[i]->GetAttrId();
            AttrFieldValuePtr attrValue(new AttrFieldValue);
            attrValue->ReserveBuffer(attrPatchReader->GetMaxPatchItemLen());
            if ((size_t)attrId >= mPatchValues.size())
            {
                mPatchValues.resize(attrId + 1);
            }
            mPatchValues[attrId] = attrValue;
            mPatchItemCount += attrPatchReader->GetPatchItemCount();            
            mHeap.push(make_pair(attrPatchReader.release(), attrConfVec[i]->GetAttrId()));
        }
    }
}

AttributePatchReader* PackAttributePatchReader::CreateSubAttributePatchReader(
    const AttributeConfigPtr& attrConfig, const PartitionDataPtr& partitionData,
    segmentid_t segmentId)
{
    unique_ptr<AttributePatchReader> patchReader(
            AttributeSegmentPatchIteratorCreator::Create(attrConfig));

    if (!patchReader)
    {
        return NULL;
    }
    patchReader->Init(partitionData, segmentId);
    if (!patchReader->HasNext())
    {
        patchReader.reset();
        return NULL;
    }
    return patchReader.release();
}

size_t PackAttributePatchReader::Next(
    docid_t& docId, uint8_t* buffer, size_t bufferLen)
{
    assert(HasNext());
    PackAttributeFormatter::PackAttributeFields patchFields;
    do
    {
        PatchReaderItem readerItem = mHeap.top();
        mHeap.pop();
        unique_ptr<AttributePatchReader> attrPatchReader(readerItem.first);
        assert(attrPatchReader);
        AttrFieldValuePtr attrValue = mPatchValues[readerItem.second];
        assert(attrValue);
        size_t len = attrPatchReader->Next(
                docId, attrValue->Data(), attrValue->BufferLength());
        patchFields.push_back(make_pair(readerItem.second,
                        ConstString((const char*)attrValue->Data(), len)));
        attrPatchReader.release();
        PushBackToHeap(readerItem);
    }
    while(!mHeap.empty() && mHeap.top().first->GetCurrentDocId() == docId);
    return PackAttributeFormatter::EncodePatchValues(
            patchFields, buffer, bufferLen);
}

void PackAttributePatchReader::PushBackToHeap(const PatchReaderItem& readerItem)
{
    unique_ptr<AttributePatchReader> attrPatchReader(readerItem.first);
    if (attrPatchReader->HasNext())
    {
        attrPatchReader.release();
        mHeap.push(readerItem);
    }
    else
    {
        attrPatchReader.reset();
    }     
}

size_t PackAttributePatchReader::Seek(
        docid_t docId, uint8_t* value, size_t maxLen)
{
    PackAttributeFormatter::PackAttributeFields patchFields;
    while(!mHeap.empty() && mHeap.top().first->GetCurrentDocId() <= docId)
    {
        PatchReaderItem readerItem = mHeap.top();
        mHeap.pop();
        unique_ptr<AttributePatchReader> attrPatchReader(readerItem.first);
        assert(attrPatchReader);
        AttrFieldValuePtr attrValue = mPatchValues[readerItem.second];
        assert(attrValue);

        size_t len = attrPatchReader->Seek(
                docId, attrValue->Data(), attrValue->BufferLength());
        if (len > 0)
        {
            patchFields.push_back(
                make_pair(readerItem.second,
                        ConstString((const char*)attrValue->Data(), len)));
        }
        attrPatchReader.release();
        PushBackToHeap(readerItem);
    }
    return PackAttributeFormatter::EncodePatchValues(
            patchFields, value, maxLen);
}

uint32_t PackAttributePatchReader::GetMaxPatchItemLen() const
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
            patchFields.push_back(make_pair(INVALID_ATTRID, 
                            ConstString((const char*)mPatchValues[i]->Data(), 
                                    buffSize)));
        }
    }
    return PackAttributeFormatter::GetEncodePatchValueLen(patchFields);
}

IE_NAMESPACE_END(index);

