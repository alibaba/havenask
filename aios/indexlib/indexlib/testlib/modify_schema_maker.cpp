#include "indexlib/testlib/modify_schema_maker.h"
#include "indexlib/config/impl/schema_modify_operation_impl.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/schema_configurator.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(testlib);
IE_LOG_SETUP(testlib, ModifySchemaMaker);


void ModifySchemaMaker::AddModifyOperations(
        const IndexPartitionSchemaPtr& schema,
        const string& deleteIndexInfo, 
        const string& addFieldNames,
        const string& addIndexNames,
        const string& addAttributeNames)
{
    assert(schema->GetTableType() == tt_index);

    IndexPartitionSchemaPtr cloneSchema(schema->Clone());
    cloneSchema->GET_IMPL()->SetModifySchemaMutable();
    
    SchemaMaker::MakeFieldConfigSchema(cloneSchema, SchemaMaker::SplitToStringVector(addFieldNames));
    FieldSchemaPtr fieldSchema = SchemaMaker::MakeFieldSchema(addFieldNames);
    vector<FieldConfigPtr> fieldConfigs;
    for (size_t i = 0; i < fieldSchema->GetFieldCount(); i++)
    {
        fieldConfigs.push_back(fieldSchema->GetFieldConfig(i));
    }
    
    vector<IndexConfigPtr> indexConfigs = SchemaMaker::MakeIndexConfigs(addIndexNames, cloneSchema);
    vector<string> delFields;
    vector<string> delIndexs;
    vector<string> delAttrs;
    
    vector<vector<string>> infos;
    StringUtil::fromString(deleteIndexInfo, infos, "=", ";");
    for (auto &info : infos)
    {
        assert(info.size() == 2);
        if (info[0] == "fields")
        {
            StringUtil::split(delFields, info[1], ",");
        }
        if (info[0] == "indexs")
        {
            StringUtil::split(delIndexs, info[1], ",");
        }
        if (info[0] == "attributes")
        {
            StringUtil::split(delAttrs, info[1], ",");            
        }
    }

    vector<string> addAttrs;
    StringUtil::split(addAttrs, addAttributeNames, ";");
    vector<AttributeConfigPtr> attrConfigs;
    for (auto& attrName : addAttrs)
    {
        FieldConfigPtr field(new FieldConfig(attrName, ft_int32, false));
        AttributeConfigPtr attrConf(new AttributeConfig);
        attrConf->Init(field);
        attrConfigs.push_back(attrConf);
    }

    SchemaModifyOperationImpl op;
    op.mDeleteFields = delFields;
    op.mDeleteIndexs = delIndexs;
    op.mDeleteAttrs = delAttrs;
    op.mAddFields = fieldConfigs;
    op.mAddIndexs = indexConfigs;
    op.mAddAttrs = attrConfigs;

    Any any = ToJson(op);
    IndexPartitionSchemaImplPtr impl = schema->GET_IMPL();
    impl->SetBaseSchemaImmutable();
    SchemaConfigurator::LoadOneModifyOperation(any, *impl);
}


IE_NAMESPACE_END(testlib);

