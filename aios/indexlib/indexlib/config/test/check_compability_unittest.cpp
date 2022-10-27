#include <fstream>
#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "indexlib/test/test.h"
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(config);

class CheckCompabilityTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(CheckCompabilityTest);
    void CaseSetUp() override
    {
        mRawSchemaFile = string(TEST_DATA_PATH) + "check_me.json";
        mRawSchema = LoadSchema(mRawSchemaFile);

        string dataFile = string(TEST_DATA_PATH) + "modified.json";
        if (FileSystemWrapper::IsExist(dataFile))
        {
            FileSystemWrapper::DeleteFile(string(TEST_DATA_PATH) + "modified.json");
        }
    }

    void CaseTearDown() override
    {
        string dataFile = string(TEST_DATA_PATH) + "modified.json";
        if (FileSystemWrapper::IsExist(dataFile))
        {
            FileSystemWrapper::DeleteFile(string(TEST_DATA_PATH) + "modified.json");
        }
    }

    void TestCaseForCompability()
    {
        /**
         * add a new_field, a new_index, a new_field to attribute and summary 
         * this new config should be compatible with check_me.json
         */
        INDEXLIB_TEST_TRUE(mRawSchema.get() != NULL);
        string compatibleSchemaFile = string(TEST_DATA_PATH) + "compatible_with_check_me.json";
        IndexPartitionSchemaPtr compatibleSchema = LoadSchema(compatibleSchemaFile);
        mRawSchema->AssertCompatible(*compatibleSchema);
    }

    void TestCaseForInCompabilityWithFieldIdChanged()
    {
        string schemaFile = string(TEST_DATA_PATH) + "incompatible_with_check_me_1.json";
        InternalTestForInCompability(schemaFile);
    }

    void TestCaseForInCompabilityWithFieldTypeChanged()
    {
        string schemaFile = string(TEST_DATA_PATH) + "incompatible_with_check_me_2.json";
        InternalTestForInCompability(schemaFile);
    }

    void TestCaseForInCompabilityWithAttrIdChanged()
    {
        string schemaFile = string(TEST_DATA_PATH) + "incompatible_with_check_me_3.json";
        InternalTestForInCompability(schemaFile);
    }

    void TestCaseForInCompabilityWithSummaryIdChanged()
    {
        string schemaFile = string(TEST_DATA_PATH) + "incompatible_with_check_me_4.json";
        InternalTestForInCompability(schemaFile);
    }

    void TestCaseForCompabilityWithAddVirtualField()
    {
        string schemaFile = string(TEST_DATA_PATH) + "index_engine_example.json";
        IndexPartitionSchemaPtr rawSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(rawSchema != NULL);

        size_t fieldCount = rawSchema->GetFieldSchema()->GetFieldCount();
        IndexPartitionSchemaPtr modifiedSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(modifiedSchema != NULL);

        FieldConfigPtr fieldConfig = modifiedSchema->AddFieldConfig("virtual", 
                ft_long, false, true);
        INDEXLIB_TEST_TRUE(fieldConfig != NULL);
        INDEXLIB_TEST_EQUAL((fieldid_t)fieldCount, fieldConfig->GetFieldId());
        INDEXLIB_TEST_EQUAL("virtual", fieldConfig->GetFieldName());
        INDEXLIB_TEST_EQUAL(ft_long, fieldConfig->GetFieldType());
        INDEXLIB_TEST_EQUAL(true, fieldConfig->IsVirtual());
        INDEXLIB_TEST_EQUAL(fieldCount+1, modifiedSchema->GetFieldSchema()->GetFieldCount());

        rawSchema->AssertCompatible(*modifiedSchema);
    }

    void TestCaseForCompabilityWithAddVirtualIndex()
    {
        string schemaFile = string(TEST_DATA_PATH) + "index_engine_example.json";
        IndexPartitionSchemaPtr rawSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(rawSchema != NULL);

        size_t fieldCount = rawSchema->GetFieldSchema()->GetFieldCount();
        size_t indexCount = rawSchema->GetIndexSchema()->GetIndexCount();
        IndexPartitionSchemaPtr modifiedSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(modifiedSchema != NULL);

        FieldConfigPtr fieldConfig = modifiedSchema->AddFieldConfig("virtual", 
                ft_long, false, true);
        INDEXLIB_TEST_TRUE(fieldConfig != NULL);

        IndexConfigPtr indexConfig = 
            IndexConfigCreator::CreateSingleFieldIndexConfig("virtual", 
                    it_number, "virtual", "",  true, 
                    modifiedSchema->GetFieldSchema());
        modifiedSchema->AddIndexConfig(indexConfig);
        INDEXLIB_TEST_TRUE(indexConfig != NULL);

        INDEXLIB_TEST_EQUAL((fieldid_t)fieldCount, fieldConfig->GetFieldId());
        INDEXLIB_TEST_EQUAL("virtual", fieldConfig->GetFieldName());
        INDEXLIB_TEST_EQUAL(ft_long, fieldConfig->GetFieldType());
        INDEXLIB_TEST_EQUAL(true, fieldConfig->IsVirtual());
        INDEXLIB_TEST_EQUAL(fieldCount+1, modifiedSchema->GetFieldSchema()->GetFieldCount());

        INDEXLIB_TEST_EQUAL((indexid_t)indexCount, indexConfig->GetIndexId());
        INDEXLIB_TEST_EQUAL(true, indexConfig->IsVirtual());
        INDEXLIB_TEST_EQUAL(indexCount+1, modifiedSchema->GetIndexSchema()->GetIndexCount());

        rawSchema->AssertCompatible(*modifiedSchema);
    }

    void TestCaseForInCompabilityWithAddNonVirtualFieldForIndex()
    {
        string schemaFile = string(TEST_DATA_PATH) + "index_engine_example.json";
        IndexPartitionSchemaPtr rawSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(rawSchema != NULL);

        IndexPartitionSchemaPtr modifiedSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(modifiedSchema != NULL);

        bool exceptionCaught = false;
        IndexConfigPtr indexConfig = 
            IndexConfigCreator::CreateSingleFieldIndexConfig("virtual", 
                    it_number, "user_id", "",  true, 
                    modifiedSchema->GetFieldSchema());
        modifiedSchema->AddIndexConfig(indexConfig);
        try 
        {
            IndexSchemaPtr indexSchema = modifiedSchema->GetIndexSchema();
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("virtual");
            indexConfig->Check();
            
        }
        catch (SchemaException& e)
        {
            exceptionCaught = true;
        }
        INDEXLIB_TEST_TRUE(exceptionCaught);
    }

private:
    IndexPartitionSchemaPtr LoadSchema(const string &schemaPath)
    {
        ifstream in(schemaPath.c_str());
        string line;
        string jsonString;
        while (getline(in, line))
        {
            jsonString += line;
        }
        Any any = ParseJson(jsonString);
        IndexPartitionSchemaPtr schema (new IndexPartitionSchema("noname"));
        FromJson(*schema, any);
        return schema;
    }

    void InternalTestForInCompability(const string &schemaFile)
    {
        INDEXLIB_TEST_TRUE(mRawSchema.get() != NULL);
        IndexPartitionSchemaPtr compatibleSchema = LoadSchema(schemaFile);
        const IndexPartitionSchemaImplPtr& compatibleImpl = compatibleSchema->GetImpl();
        const IndexPartitionSchemaImplPtr& rawImpl = mRawSchema->GetImpl();
        INDEXLIB_EXPECT_EXCEPTION(
            rawImpl->DoAssertCompatible(*(compatibleImpl.get())),
            AssertEqualException);
        mRawSchema->AssertCompatible(*compatibleSchema);
    }

private:
    string mRawSchemaFile;
    IndexPartitionSchemaPtr mRawSchema;
};

INDEXLIB_UNIT_TEST_CASE(CheckCompabilityTest, TestCaseForCompability);
INDEXLIB_UNIT_TEST_CASE(CheckCompabilityTest, TestCaseForInCompabilityWithFieldIdChanged);
INDEXLIB_UNIT_TEST_CASE(CheckCompabilityTest, TestCaseForInCompabilityWithFieldTypeChanged);
INDEXLIB_UNIT_TEST_CASE(CheckCompabilityTest, TestCaseForInCompabilityWithAttrIdChanged);
INDEXLIB_UNIT_TEST_CASE(CheckCompabilityTest, TestCaseForInCompabilityWithSummaryIdChanged);
INDEXLIB_UNIT_TEST_CASE(CheckCompabilityTest, TestCaseForCompabilityWithAddVirtualField);
INDEXLIB_UNIT_TEST_CASE(CheckCompabilityTest, TestCaseForCompabilityWithAddVirtualIndex);
INDEXLIB_UNIT_TEST_CASE(CheckCompabilityTest, TestCaseForInCompabilityWithAddNonVirtualFieldForIndex);

IE_NAMESPACE_END(config);
