#include "indexlib/partition/join_cache/join_cache_initializer_creator.h"
#include "indexlib/partition/join_cache/join_cache_initializer.h"
#include "indexlib/partition/table_reader_container.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer_creator.h"
#include <autil/StringUtil.h>
#include "indexlib/partition/online_partition.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, JoinCacheInitializerCreator);

JoinCacheInitializerCreator::JoinCacheInitializerCreator() 
{
}

JoinCacheInitializerCreator::~JoinCacheInitializerCreator() 
{
}


bool JoinCacheInitializerCreator::Init(
        const string &joinFieldName,
        bool isSubField,
        IndexPartitionSchemaPtr schema,
        OnlinePartition* auxPart,
        versionid_t auxReaderIncVersionId)
{
    // mAuxPartition = auxPartition;
    // IndexPartitionSchemaPtr schema = mainPartition->GetSchema();
    if (isSubField) {
        const IndexPartitionSchemaPtr &subSchema = schema->GetSubIndexPartitionSchema();
        if (!subSchema) {
            return false;
        }
        schema = subSchema;
    }

    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();
    if (attrSchema) {
        mJoinFieldAttrConfig = attrSchema->GetAttributeConfig(joinFieldName);
    }
    if (!mJoinFieldAttrConfig) {
        IE_LOG(ERROR, "joinField [%s] not exist in attribute!", 
               joinFieldName.c_str());
        return false;
    }
    mAuxPart = auxPart;
    mAuxReaderIncVersionId = auxReaderIncVersionId;
    return true;
}

AttributeValueInitializerPtr JoinCacheInitializerCreator::Create(
        const PartitionDataPtr& partitionData)
{
    if (!mJoinFieldAttrConfig)
    {
        IE_LOG(ERROR, "joinField not in attribute!");
        return AttributeValueInitializerPtr();
    }

    AttributeReaderPtr attrReader = 
        AttributeReaderFactory::CreateAttributeReader(
                mJoinFieldAttrConfig, partitionData);
    if (!attrReader)
    {
        IE_LOG(ERROR, "create attribute reader for joinField [%s] fail!", 
               mJoinFieldAttrConfig->GetAttrName().c_str());
        return AttributeValueInitializerPtr();
    }

    JoinCacheInitializerPtr initializer(new JoinCacheInitializer);
    initializer->Init(attrReader, mAuxPart->GetReader(mAuxReaderIncVersionId));
    return initializer;
}

AttributeValueInitializerPtr JoinCacheInitializerCreator::Create(
        const InMemorySegmentReaderPtr& inMemSegmentReader)
{
    if (!mJoinFieldAttrConfig)
    {
        IE_LOG(ERROR, "joinField not in attribute!");
        return AttributeValueInitializerPtr();
    }

    FieldConfigPtr fieldConfig(new FieldConfig(
                    "join_docid_cache", ft_int32, false, true, false));
    docid_t defaultValue = INVALID_DOCID;
    string defaultJoinValueStr = StringUtil::toString(defaultValue);
    fieldConfig->SetDefaultValue(defaultJoinValueStr);

    DefaultAttributeValueInitializerCreator creator(fieldConfig);
    return creator.Create(inMemSegmentReader);
}


std::string JoinCacheInitializerCreator::GenerateVirtualAttributeName(
        const string& prefixName, uint64_t auxPartitionIdentifier,
        const string& joinField, versionid_t vertionid)
{
    return prefixName + joinField + "_" + StringUtil::toString(auxPartitionIdentifier)
        + "_" + StringUtil::toString(vertionid);
}

IE_NAMESPACE_END(partition);
