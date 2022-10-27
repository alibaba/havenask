#include "indexlib/document/document_rewriter/pack_attribute_appender.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/config/kkv_index_config.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, PackAttributeAppender);

bool PackAttributeAppender::Init(const IndexPartitionSchemaPtr& schema, regionid_t regionId)
{
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema(regionId);
    if (!attrSchema)
    {
        IE_LOG(INFO, "no attribute schema defined in schema: %s",
               schema->GetSchemaName().c_str());
        return false;
    }

    TableType tableType = schema->GetTableType();
    if (tableType == tt_kkv || tableType == tt_kv)
    {
        const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(regionId);
        KVIndexConfigPtr kvConfig = DYNAMIC_POINTER_CAST(KVIndexConfig,
                indexSchema->GetPrimaryKeyIndexConfig());
        if (!kvConfig)
        {
            return false;
        }
        const ValueConfigPtr& valueConfig = kvConfig->GetValueConfig();
        if (tableType == tt_kv && schema->GetRegionCount() == 1
            && valueConfig->GetAttributeCount() == 1
            && !valueConfig->GetAttributeConfig(0)->IsMultiValue()
            && (valueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string))
        {
            IE_LOG(INFO, "No need PackAttributeAppender for region [%d]!",
                   kvConfig->GetRegionId());
            return false;
        }

        if (tableType == tt_kkv)
        {
            KKVIndexConfigPtr kkvConfig =
                DYNAMIC_POINTER_CAST(KKVIndexConfig,
                                     indexSchema->GetPrimaryKeyIndexConfig());
            if (kkvConfig->OptimizedStoreSKey())
            {
                string suffixKeyFieldName = kkvConfig->GetSuffixFieldName();
                fieldid_t fieldId = schema->GetFieldSchema(regionId)->GetFieldId(suffixKeyFieldName);
                mClearFields.push_back(fieldId);
            }
        }
        return InitOnePackAttr(valueConfig->CreatePackAttributeConfig());
    }
    
    size_t packConfigCount = attrSchema->GetPackAttributeCount();
    if (packConfigCount == 0)
    {
        return false;
    }
    
    mPackFormatters.reserve(packConfigCount);

    for (packattrid_t packId = 0; (size_t)packId < packConfigCount; ++packId)
    {
        if (!InitOnePackAttr(attrSchema->GetPackAttributeConfig(packId)))
        {
            return false;
        }
    }
    return true;
}

bool PackAttributeAppender::InitOnePackAttr(const PackAttributeConfigPtr& packAttrConfig)
{
    assert(packAttrConfig);
    const vector<AttributeConfigPtr>& subAttrConfs =
        packAttrConfig->GetAttributeConfigVec();

    for (size_t i = 0; i < subAttrConfs.size(); ++i)
    {
        mInPackFields.push_back(subAttrConfs[i]->GetFieldId());
        mClearFields.push_back(subAttrConfs[i]->GetFieldId());
    }
        
    PackAttributeFormatterPtr packFormatter(new PackAttributeFormatter());
    if (!packFormatter->Init(packAttrConfig))
    {
        return false;
    }
    mPackFormatters.push_back(packFormatter);
    return true;
}

bool PackAttributeAppender::AppendPackAttribute(
    const AttributeDocumentPtr& attrDocument, Pool* pool)
{
    if (attrDocument->GetPackFieldCount() > 0)
    {
        IE_LOG(DEBUG, "attributes have already been packed");
        return CheckPackAttrFields(attrDocument);
    }
    
    for (size_t i = 0; i < mPackFormatters.size(); ++i)
    {
        const PackAttributeFormatter::FieldIdVec& fieldIdVec =
            mPackFormatters[i]->GetFieldIds();
        vector<ConstString> fieldVec;
        fieldVec.reserve(fieldIdVec.size());
        for (auto fid : fieldIdVec)
        {
            fieldVec.push_back(attrDocument->GetField(fid));
        }
        
        ConstString packedAttrField = mPackFormatters[i]->Format(fieldVec, pool);
        if (packedAttrField.empty())
        {
            IE_LOG(WARN, "format pack attr field [%lu] fail!", i);
            ERROR_COLLECTOR_LOG(WARN, "format pack attr field [%lu] fail!", i);
            return false;
        }
        attrDocument->SetPackField(packattrid_t(i), packedAttrField);
    }
    attrDocument->ClearFields(mClearFields);
    return true;
}

bool PackAttributeAppender::CheckPackAttrFields(const AttributeDocumentPtr& attrDocument)
{
    assert(attrDocument->GetPackFieldCount() > 0);
    if (attrDocument->GetPackFieldCount() != mPackFormatters.size())
    {
        IE_LOG(WARN, "check pack field count [%lu:%lu] fail!",
               attrDocument->GetPackFieldCount(),  mPackFormatters.size());
        return false;
    }

    for (size_t i = 0; i < mPackFormatters.size(); i++)
    {
        const ConstString& packField = attrDocument->GetPackField((packattrid_t)i);
        if (packField.empty())
        {
            IE_LOG(WARN, "pack field [%lu] is empty!", i);
            return false;
        }
        if (!mPackFormatters[i]->CheckLength(packField))
        {
            IE_LOG(WARN, "check length [%lu] for encode pack field [%lu] fail!",
                   packField.length(), i);
            return false;
        }
    }
    return true;    
}

IE_NAMESPACE_END(document);

