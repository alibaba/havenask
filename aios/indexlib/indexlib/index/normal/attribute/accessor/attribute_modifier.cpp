#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"
#include "indexlib/index/normal/attribute/attribute_update_bitmap.h"
#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/config/index_partition_schema.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeModifier);

AttributeModifier::AttributeModifier(const IndexPartitionSchemaPtr& schema,
                                     util::BuildResourceMetrics* buildResourceMetrics)
    : mSchema(schema)
    , mBuildResourceMetrics(buildResourceMetrics)
{
    InitAttributeConvertorMap(schema);
}

AttributeModifier::~AttributeModifier()
{
    mPackUpdateBitmapVec.clear();
}

void AttributeModifier::InitAttributeConvertorMap(
        const IndexPartitionSchemaPtr& schema)
{
    size_t fieldCount = schema->GetFieldSchema()->GetFieldCount();
    mFieldId2ConvertorMap.resize(fieldCount);
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema();
    if (!attrSchema)
    {
        return;
    }

    AttributeSchema::Iterator iter = attrSchema->Begin();
    for (; iter != attrSchema->End(); iter++)
    {
        const FieldConfigPtr& fieldConfig = (*iter)->GetFieldConfig();
        AttributeConvertorPtr attrConvertor(
                AttributeConvertorFactory::GetInstance()
                ->CreateAttrConvertor(fieldConfig));
        mFieldId2ConvertorMap[fieldConfig->GetFieldId()] = attrConvertor;
    }
}

void AttributeModifier::InitPackAttributeUpdateBitmap(
    const PartitionDataPtr& partitionData)
{
    assert(partitionData);
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema)
    {
        return;
    }
    
    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    for (; packIter != packAttrConfigs->End(); packIter++)
    {
        AttributeUpdateBitmapPtr packAttrUpdateBitmap;
        packAttrUpdateBitmap.reset(new AttributeUpdateBitmap());
        packAttrUpdateBitmap->Init(partitionData, mBuildResourceMetrics);
        mPackUpdateBitmapVec.push_back(packAttrUpdateBitmap);
    }
}

void AttributeModifier::DumpPackAttributeUpdateInfo(
    const DirectoryPtr& attrDirectory) const
{
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    if (!attrSchema)
    {
        return;
    }
    
    auto packAttrConfigs = attrSchema->CreatePackAttrIterator();
    auto packIter = packAttrConfigs->Begin();
    size_t i = 0;
    for (; packIter != packAttrConfigs->End(); packIter++)    
    {
        const PackAttributeConfigPtr& packAttrConfig = *packIter;
        const AttributeUpdateBitmapPtr& packAttrUpdateBitmap = mPackUpdateBitmapVec[i];
        const string& packName = packAttrConfig->GetAttrName();
        DirectoryPtr packAttrDir = attrDirectory->MakeDirectory(packName);
        packAttrUpdateBitmap->Dump(packAttrDir);
        i++;
    }
}

IE_NAMESPACE_END(index);

