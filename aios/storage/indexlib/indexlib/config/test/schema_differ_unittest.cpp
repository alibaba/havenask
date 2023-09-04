#include "indexlib/config/test/schema_differ_unittest.h"

#include "indexlib/config/index_partition_schema_maker.h"
#include "indexlib/test/modify_schema_maker.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, SchemaDifferTest);

SchemaDifferTest::SchemaDifferTest() {}

SchemaDifferTest::~SchemaDifferTest() {}

void SchemaDifferTest::CaseSetUp() {}

void SchemaDifferTest::CaseTearDown() {}

void SchemaDifferTest::TestSimpleProcess()
{
    string errorMsg;
    {
        IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32", "pk:PRIMARYKEY64:field1",
                                              "field1;field2", "");
        IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(newSchema,
                                              "field1:long;field2:uint32;"
                                              "field3:string;field4:uint32",
                                              "pk:PRIMARYKEY64:field1", "field1;field2;field3;field4", "");
        vector<AttributeConfigPtr> alterAttributes;
        vector<IndexConfigPtr> alterIndexes;
        ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes));

        newSchema->SetSchemaVersionId(1);
        ASSERT_TRUE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes));
        ASSERT_EQ(2u, alterAttributes.size());
        ASSERT_TRUE(alterIndexes.empty());
    }

    {
        IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32", "pk:PRIMARYKEY64:field1;", "",
                                              "");
        IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(newSchema, "field1:long;field2:uint32;field3:text",
                                              "pk:PRIMARYKEY64:field1;text1:text:field3", "", "");
        vector<AttributeConfigPtr> alterAttributes;
        vector<IndexConfigPtr> alterIndexes;
        newSchema->SetSchemaVersionId(1);
        ASSERT_TRUE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
        ASSERT_EQ(1u, alterIndexes.size());
        ASSERT_TRUE(alterAttributes.empty());
    }
}

void SchemaDifferTest::TestAddSummary()
{
    string errorMsg;
    IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32", "pk:PRIMARYKEY64:field1;",
                                          "field1;field2", "");
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(newSchema, "field1:long;field2:uint32;field3:string",
                                          "pk:PRIMARYKEY64:field1;", "field1;field2;", "field3");
    vector<AttributeConfigPtr> alterAttributes;
    vector<IndexConfigPtr> alterIndexes;

    newSchema->SetSchemaVersionId(1);
    ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
}

void SchemaDifferTest::TestRemoveFieldFail()
{
    string errorMsg;
    string field = "field1:long;field2:uint32;field3:uint32;field4:uint32;field5:uint32";
    string index = "pk:PRIMARYKEY64:field1;index:number:field1";
    string attribute = "field3;pack1:field1,field2";
    string summary = "field4;field5;field3";
    IndexPartitionSchemaPtr oldSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
    {
        // delete single pack attribute fail
        string attribute = "field3;pack1:field2";
        IndexPartitionSchemaPtr newSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
        newSchema->SetSchemaVersionId(1);
        ASSERT_FALSE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
    }
    {
        // delete pk index
        string index = "index:number:field1";
        IndexPartitionSchemaPtr newSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
        newSchema->SetSchemaVersionId(1);
        ASSERT_FALSE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
    }
    {
        // delete summary
        string summary = "field5";
        IndexPartitionSchemaPtr newSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
        newSchema->SetSchemaVersionId(1);
        ASSERT_FALSE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
    }
    {
        // delete attribute in summary
        string attribute = "pack1:field1,field2";
        IndexPartitionSchemaPtr newSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
        newSchema->SetSchemaVersionId(1);
        ASSERT_FALSE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
    }
    {
        // delete summary in attribute
        string summary = "field4;field5";
        IndexPartitionSchemaPtr newSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
        newSchema->SetSchemaVersionId(1);
        ASSERT_TRUE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
    }
    {
        // delete attribute and summary
        string attribute = "pack1:field1,field2";
        string summary = "field4;field5";
        IndexPartitionSchemaPtr newSchema = SchemaMaker::MakeSchema(field, index, attribute, summary);
        newSchema->SetSchemaVersionId(1);
        ASSERT_TRUE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
    }
}

void SchemaDifferTest::TestRemoveField()
{
    string errorMsg;
    {
        // remove attribute field2, add attribute field3,field4
        IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32", "pk:PRIMARYKEY64:field1",
                                              "field1;field2", "");
        IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(newSchema,
                                              "field1:long;"
                                              "field3:string;field4:uint32",
                                              "pk:PRIMARYKEY64:field1", "field1;field3;field4", "");
        vector<AttributeConfigPtr> alterAttributes;
        vector<IndexConfigPtr> alterIndexes;

        newSchema->SetSchemaVersionId(1);
        ASSERT_TRUE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
        ASSERT_TRUE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
        ASSERT_EQ(2u, alterAttributes.size());
        ASSERT_EQ(0u, alterIndexes.size());
        vector<AttributeConfigPtr> deleteAttributes;
        vector<IndexConfigPtr> deleteIndexes;
        ASSERT_TRUE(
            SchemaDiffer::CalculateDeleteFields(oldSchema, newSchema, deleteAttributes, deleteIndexes, errorMsg));
        ASSERT_EQ(1u, deleteAttributes.size());
        ASSERT_EQ(0u, deleteIndexes.size());
    }
    {
        // remove index field3, add attribute field4
        IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32;field3:text",
                                              "pk:PRIMARYKEY64:field1;text1:text:field3", "field1;field2", "");
        IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(newSchema,
                                              "field1:long;"
                                              "field2:uint32;field4:uint32",
                                              "pk:PRIMARYKEY64:field1", "field1;field2;field4", "");
        vector<AttributeConfigPtr> alterAttributes;
        vector<IndexConfigPtr> alterIndexes;

        newSchema->SetSchemaVersionId(1);
        ASSERT_TRUE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
        ASSERT_TRUE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
        ASSERT_EQ(1u, alterAttributes.size());
        ASSERT_EQ(0u, alterIndexes.size());
        vector<AttributeConfigPtr> deleteAttributes;
        vector<IndexConfigPtr> deleteIndexes;
        ASSERT_TRUE(
            SchemaDiffer::CalculateDeleteFields(oldSchema, newSchema, deleteAttributes, deleteIndexes, errorMsg));
        ASSERT_EQ(0u, deleteAttributes.size());
        ASSERT_EQ(1u, deleteIndexes.size());
    }

    {
        // remove index field3, attribute field2
        IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32;field3:text",
                                              "pk:PRIMARYKEY64:field1;text1:text:field3", "field1;field2", "");
        IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(newSchema, "field1:long", "pk:PRIMARYKEY64:field1", "field1", "");
        vector<AttributeConfigPtr> alterAttributes;
        vector<IndexConfigPtr> alterIndexes;

        newSchema->SetSchemaVersionId(1);
        ASSERT_TRUE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
        ASSERT_TRUE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
        ASSERT_EQ(0u, alterAttributes.size());
        ASSERT_EQ(0u, alterIndexes.size());
        vector<AttributeConfigPtr> deleteAttributes;
        vector<IndexConfigPtr> deleteIndexes;
        ASSERT_TRUE(
            SchemaDiffer::CalculateDeleteFields(oldSchema, newSchema, deleteAttributes, deleteIndexes, errorMsg));
        ASSERT_EQ(1u, deleteAttributes.size());
        ASSERT_EQ(1u, deleteIndexes.size());
    }
}

void SchemaDifferTest::TestWrongTableType()
{
    string errorMsg;

    IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32", "pk:PRIMARYKEY64:field1",
                                          "field1;field2", "");
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(newSchema,
                                          "field1:long;field2:uint32;"
                                          "field3:string;field4:uint32",
                                          "pk:PRIMARYKEY64:field1", "field1;field2;field3;field4", "");
    vector<AttributeConfigPtr> alterAttributes;
    vector<IndexConfigPtr> alterIndexes;
    newSchema->SetSchemaVersionId(1);
    ASSERT_TRUE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));

    oldSchema->SetSchemaName("newName");
    ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
    ASSERT_TRUE(errorMsg.find("different table name") != string::npos);

    newSchema->SetSchemaName("newName");
    oldSchema->SetTableType("kv");
    ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
    ASSERT_TRUE(errorMsg.find("different table type") != string::npos);

    newSchema->SetTableType("kv");
    ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
    ASSERT_TRUE(errorMsg.find("update schema for [kv] table") != string::npos);
}

void SchemaDifferTest::TestTruncateIndex()
{
    string errorMsg;
    IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32", "pk:PRIMARYKEY64:field1;", "", "");
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(newSchema, "field1:long;field2:uint32;field3:text",
                                          "pk:PRIMARYKEY64:field1;text1:text:field3", "", "");
    vector<AttributeConfigPtr> alterAttributes;
    vector<IndexConfigPtr> alterIndexes;
    newSchema->SetSchemaVersionId(1);

    IndexConfigPtr indexConf = newSchema->GetIndexSchema()->GetIndexConfig("text1");
    ASSERT_TRUE(indexConf != NULL);
    indexConf->SetHasTruncateFlag(true);
    ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
    ASSERT_TRUE(errorMsg.find("which need truncate") != string::npos);
}

void SchemaDifferTest::TestSubIndexSchema()
{
    string errorMsg;
    IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
    IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32", "pk:PRIMARYKEY64:field1;", "", "");
    IndexPartitionSchemaPtr newSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(newSchema, "field1:long;field2:uint32;field3:text",
                                          "pk:PRIMARYKEY64:field1;text1:text:field3", "", "");
    newSchema->SetSubIndexPartitionSchema(subSchema);

    vector<AttributeConfigPtr> alterAttributes;
    vector<IndexConfigPtr> alterIndexes;

    newSchema->SetSchemaVersionId(1);
    ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
    ASSERT_TRUE(errorMsg.find("sub doc schema") != string::npos);
}

void SchemaDifferTest::TestCustomizedIndex()
{
    string errorMsg;
    IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
    IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field1:long;field2:uint32", "pk:PRIMARYKEY64:field1;", "", "");
    IndexPartitionSchemaPtr newSchema =
        SchemaMaker::MakeSchema("field1:long;field2:uint32;demo_field:raw",
                                "pk:PRIMARYKEY64:field1;test_customize:customized:demo_field", "", "");

    vector<AttributeConfigPtr> alterAttributes;
    vector<IndexConfigPtr> alterIndexes;
    newSchema->SetSchemaVersionId(1);

    ASSERT_TRUE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
}

void SchemaDifferTest::TestPackAttribute()
{
    string errorMsg;
    IndexPartitionSchemaPtr oldSchema =
        SchemaMaker::MakeSchema("field1:long;field2:uint32", "pk:PRIMARYKEY64:field1;", "pack1:field1,field2", "");

    {
        // new attribute in current pack attribute
        IndexPartitionSchemaPtr newSchema =
            SchemaMaker::MakeSchema("field1:long;field2:uint32;field3:uint64;field4:uint16", "pk:PRIMARYKEY64:field1",
                                    "pack1:field1,field2,field3", "");
        newSchema->SetSchemaVersionId(1);

        vector<AttributeConfigPtr> alterAttributes;
        vector<IndexConfigPtr> alterIndexes;
        ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
        ASSERT_TRUE(errorMsg.find("which in pack attribute") != string::npos);
    }

    {
        // new attribute in new pack attribute
        IndexPartitionSchemaPtr newSchema =
            SchemaMaker::MakeSchema("field1:long;field2:uint32;field3:uint64;field4:uint16", "pk:PRIMARYKEY64:field1",
                                    "pack1:field1,field2;pack2:field3,field4", "");
        newSchema->SetSchemaVersionId(1);

        vector<AttributeConfigPtr> alterAttributes;
        vector<IndexConfigPtr> alterIndexes;
        ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
        ASSERT_TRUE(errorMsg.find("which in pack attribute") != string::npos);
    }
}

void SchemaDifferTest::TestPrimaryKeyIndex()
{
    string errorMsg;
    IndexPartitionSchemaPtr oldSchema =
        SchemaMaker::MakeSchema("field1:long;field2:uint32", "numberIndex:number:field1;", "", "");

    // new attribute in current pack attribute
    IndexPartitionSchemaPtr newSchema =
        SchemaMaker::MakeSchema("field1:long;field2:uint32;field3:uint64;field4:uint16",
                                "pk:PRIMARYKEY64:field4;numberIndex:number:field1", "field2;field3", "");
    newSchema->SetSchemaVersionId(1);

    vector<AttributeConfigPtr> alterAttributes;
    vector<IndexConfigPtr> alterIndexes;
    ASSERT_FALSE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, alterAttributes, alterIndexes, errorMsg));
    ASSERT_TRUE(errorMsg.find("which is primary key index") != string::npos);
}

void SchemaDifferTest::TestModifySchemaOperation()
{
    string errorMsg;
    IndexPartitionSchemaPtr oldSchema =
        SchemaMaker::MakeSchema("field1:long;field2:uint32", "numberIndex:number:field1;", "", "");

    IndexPartitionSchemaPtr newSchema(oldSchema->Clone());
    ModifySchemaMaker::AddModifyOperations(newSchema, "", "field3:uint64;field4:uint16", "", "field3;field4");
    ASSERT_TRUE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));

    IndexPartitionSchemaPtr newSchema1(oldSchema->Clone());
    ModifySchemaMaker::AddModifyOperations(newSchema1, "", "field3:uint64;field4:uint32", "", "field3;field4");

    IndexPartitionSchemaPtr newSchema2(oldSchema->Clone());
    ModifySchemaMaker::AddModifyOperations(newSchema2, "", "field3:uint64;field4:uint32", "", "field3;field4");
    ModifySchemaMaker::AddModifyOperations(newSchema2, "", "field5:string", "", "field5");

    ASSERT_FALSE(SchemaDiffer::CheckAlterFields(newSchema, newSchema2, errorMsg));
    ASSERT_TRUE(errorMsg.find("FieldType not equal") != string::npos);
    ASSERT_TRUE(SchemaDiffer::CheckAlterFields(newSchema1, newSchema2, errorMsg));
}

void SchemaDifferTest::TestUpdateFieldInModifySchema()
{
    IndexPartitionSchemaPtr oldSchema =
        SchemaMaker::MakeSchema("field1:long;field2:uint32", "numberIndex:number:field1;", "field2", "");
    IndexPartitionSchemaPtr newSchema(oldSchema->Clone());
    ModifySchemaMaker::AddModifyOperations(newSchema, "attributes=field2;indexs=numberIndex", "",
                                           "numberIndex:number:field1", "field2");

    string errorMsg;
    ASSERT_TRUE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));

    vector<AttributeConfigPtr> attrConfigs;
    vector<IndexConfigPtr> indexConfigs;

    ASSERT_TRUE(SchemaDiffer::CalculateAlterFields(oldSchema, newSchema, attrConfigs, indexConfigs));
    ASSERT_EQ(1, attrConfigs.size());
    ASSERT_EQ(1, attrConfigs[0]->GetAttrId());

    ASSERT_EQ(1, indexConfigs.size());
    ASSERT_EQ(1, indexConfigs[0]->GetIndexId());
}

void SchemaDifferTest::TestUpdateFieldInModifySchemaWithSubSchema()
{
    {
        // test unsupport modify sub schema
        IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field0:long;field2:uint32", "pk:PRIMARYKEY64:field0",
                                              "field0;field2", "");
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "sub_field0:long;sub_field2:uint32",
                                              "sub_pk:PRIMARYKEY64:sub_field0", "sub_field0;sub_field2", "");
        oldSchema->SetSubIndexPartitionSchema(subSchema);
        IndexPartitionSchemaPtr newSchema(oldSchema->Clone());
        ModifySchemaMaker::AddModifyOperations(newSchema, "", "field3:uint64;field4:uint16", "", "field3;field4");
        ModifySchemaMaker::AddModifyOperations(newSchema->GetSubIndexPartitionSchema(), "",
                                               "field3:uint64;field4:uint16", "", "field3;field4");
        string errorMsg;
        ASSERT_FALSE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
        ASSERT_TRUE(errorMsg.find("modify operation assertCompatiable") != string::npos);
    }
    {
        // test newSchema don't have sub schema
        IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field0:long;field2:uint32", "pk:PRIMARYKEY64:field0",
                                              "field0;field2", "");
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "sub_field0:long;sub_field2:uint32",
                                              "sub_pk:PRIMARYKEY64:sub_field0", "sub_field0;sub_field2", "");
        IndexPartitionSchemaPtr newSchema(oldSchema->Clone());
        oldSchema->SetSubIndexPartitionSchema(subSchema);
        ModifySchemaMaker::AddModifyOperations(newSchema, "", "field3:uint64;field4:uint16", "", "field3;field4");
        string errorMsg;
        ASSERT_FALSE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
        ASSERT_TRUE(errorMsg.find("without modify opertion not equal") != string::npos);
    }
    {
        // test normal
        IndexPartitionSchemaPtr oldSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(oldSchema, "field0:long;field2:uint32", "pk:PRIMARYKEY64:field0",
                                              "field0;field2", "");
        IndexPartitionSchemaPtr subSchema(new IndexPartitionSchema);
        IndexPartitionSchemaMaker::MakeSchema(subSchema, "sub_field0:long;sub_field2:uint32",
                                              "sub_pk:PRIMARYKEY64:sub_field0", "sub_field0;sub_field2", "");
        oldSchema->SetSubIndexPartitionSchema(subSchema);
        IndexPartitionSchemaPtr newSchema(oldSchema->Clone());
        ModifySchemaMaker::AddModifyOperations(newSchema, "", "field3:uint64;field4:uint16", "", "field3;field4");
        string errorMsg;
        ASSERT_TRUE(SchemaDiffer::CheckAlterFields(oldSchema, newSchema, errorMsg));
    }
}
}} // namespace indexlib::config
