#include "indexlib/index/normal/attribute/accessor/inplace_attribute_modifier.h"
#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/index/normal/attribute/update_field_extractor.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InplaceAttributeModifier);

InplaceAttributeModifier::InplaceAttributeModifier(
        const IndexPartitionSchemaPtr& schema,
        util::BuildResourceMetrics* buildResourceMetrics)
    : AttributeModifier(schema, buildResourceMetrics) {}
      
InplaceAttributeModifier::~InplaceAttributeModifier() 
{
}

void InplaceAttributeModifier::Init(
    const AttributeReaderContainerPtr& attrReaderContainer,
    const PartitionDataPtr& partitionData)
{
    mModifierMap.resize(mSchema->GetFieldSchema()->GetFieldCount());
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
        if (!attrConfig->IsAttributeUpdatable())
        {
            continue;
        }

        AttributeReaderPtr attrReader = 
            attrReaderContainer->GetAttributeReader(attrConfig->GetAttrName());
        assert(attrReader);
        fieldid_t fieldId = attrConfig->GetFieldId();
        mModifierMap[fieldId] = attrReader;
    }

    size_t packAttrCount = attrSchema->GetPackAttributeCount();
    mPackIdToPackFields.resize(packAttrCount);
    mPackModifierMap.resize(packAttrCount);
    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++)
    {
        const auto& packAttrConfig = *packIter;
        PackAttributeReaderPtr packAttrReader;
        packattrid_t packId = packAttrConfig->GetPackAttrId();
        packAttrReader = attrReaderContainer->GetPackAttributeReader(packId);
        assert(packAttrReader);
        packAttrReader->InitBuildResourceMetricsNode(mBuildResourceMetrics);
        mPackModifierMap[packId] = packAttrReader;
    }
    InitPackAttributeUpdateBitmap(partitionData);
}

AttributeSegmentReaderPtr InplaceAttributeModifier::GetAttributeSegmentReader(
        fieldid_t fieldId, docid_t segBaseDocId) const
{
    if ((size_t)fieldId >= mModifierMap.size())
    {
        IE_LOG(ERROR, "INVALID fieldId[%u]", fieldId);
        return AttributeSegmentReaderPtr();
    }
    const AttributeReaderPtr& attrReader = mModifierMap[fieldId];
    if (!attrReader)
    {
        IE_LOG(ERROR, "AttributeReader is NULL for field[%u]", fieldId);
        return AttributeSegmentReaderPtr();
    }
    AttributeSegmentReaderPtr segReader =
        attrReader->GetSegmentReader(segBaseDocId);
    if (!segReader)
    {
        IE_LOG(ERROR, "No matching SegmentReader for docid[%d]", segBaseDocId);
    }
    return segReader;
}


bool InplaceAttributeModifier::Update(
            docid_t docId, const AttributeDocumentPtr& attrDoc)
{
    UpdateFieldExtractor extractor(mSchema);
    if (!extractor.Init(attrDoc))
    {
        return false;
    }
    const AttributeSchemaPtr& attrSchema = mSchema->GetAttributeSchema();
    assert(attrSchema);
    UpdateFieldExtractor::Iterator iter = extractor.CreateIterator();
    while (iter.HasNext())
    {
        fieldid_t fieldId = INVALID_FIELDID;
        const ConstString& value = iter.Next(fieldId);

        const AttributeConfigPtr& attrConfig = 
            attrSchema->GetAttributeConfigByFieldId(fieldId);

        assert(attrConfig);
        PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
        if (packAttrConfig)
        {
            if (packAttrConfig->IsDisable())
            {
                continue;
            }
            mPackIdToPackFields[packAttrConfig->GetPackAttrId()].push_back(
                make_pair(attrConfig->GetAttrId(), value));
            
            const AttributeUpdateBitmapPtr& packAttrUpdateBitmap =
                mPackUpdateBitmapVec[packAttrConfig->GetPackAttrId()];
            assert(packAttrUpdateBitmap);
            if (unlikely(!packAttrUpdateBitmap))
            {
                continue;
            }
            packAttrUpdateBitmap->Set(docId);   
        }
        else
        {
            const AttributeConvertorPtr& convertor = mFieldId2ConvertorMap[fieldId];
            assert(convertor);
            AttrValueMeta meta = convertor->Decode(value);
            UpdateField(docId, fieldId, meta.data);
        }
    }
    UpdateInPackFields(docId);
    return true;
}

void InplaceAttributeModifier::UpdateInPackFields(docid_t docId)
{
    assert(mPackModifierMap.size() == mPackIdToPackFields.size());
    for (size_t i = 0; i < mPackIdToPackFields.size(); ++i)
    {
        if (mPackIdToPackFields[i].empty())
        {
            continue;
        }

        const PackAttributeReaderPtr& packAttrReader = mPackModifierMap[i];
        assert(packAttrReader);
        packAttrReader->UpdatePackFields(docId, mPackIdToPackFields[i], true);
        mPackIdToPackFields[i].clear();
    }
}

bool InplaceAttributeModifier::UpdateField(
    docid_t docId, fieldid_t fieldId, const ConstString& value)
{
    assert((size_t)fieldId < mModifierMap.size());
    const AttributeReaderPtr& attrReader = mModifierMap[fieldId];
    if (!attrReader)
    {
        return false;
    }
    return attrReader->UpdateField(
            docId, (uint8_t*)value.data(), value.size());
}

PackAttributeReaderPtr InplaceAttributeModifier::GetPackAttributeReader(
    packattrid_t packAttrId) const
{
    if ((size_t)packAttrId >= mPackModifierMap.size())
    {
        IE_LOG(ERROR, "Invalid PackAttrId[%u], return NULL PackAttributeReader", packAttrId);
        return PackAttributeReaderPtr();
    }
    if (!mPackModifierMap[packAttrId])
    {
        IE_LOG(ERROR, "no matching PackAttributeReader for packAttrId[%u]", packAttrId);
    }
    return mPackModifierMap[packAttrId];
}

bool InplaceAttributeModifier::UpdatePackField(
    docid_t docId, packattrid_t packAttrId, const ConstString& value)
{
    assert((size_t)packAttrId < mPackModifierMap.size());
    const PackAttributeReaderPtr& packAttrReader = mPackModifierMap[packAttrId];
    if (!packAttrReader)
    {
        return false;
    }
    return packAttrReader->UpdateField(
        docId, (uint8_t*)value.data(), value.size());
}

void InplaceAttributeModifier::Dump(const DirectoryPtr& dir)
{
    DirectoryPtr attrDir = dir->MakeDirectory(ATTRIBUTE_DIR_NAME);
    DumpPackAttributeUpdateInfo(attrDir);
}

IE_NAMESPACE_END(index);

