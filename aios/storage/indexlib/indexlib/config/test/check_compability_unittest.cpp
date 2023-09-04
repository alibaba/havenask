#include <fstream>

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/impl/index_partition_schema_impl.h"
#include "indexlib/config/index_config_creator.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace config {

class CheckCompabilityTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(CheckCompabilityTest);
    void CaseSetUp() override
    {
        mRawSchemaFile = GET_PRIVATE_TEST_DATA_PATH() + "check_me.json";
        mRawSchema = LoadSchema(mRawSchemaFile);

        string dataFile = GET_TEST_DATA_PATH() + "modified.json";
        if (FslibWrapper::IsExist(dataFile).GetOrThrow()) {
            FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "modified.json", DeleteOption::NoFence(false));
        }
    }

    void CaseTearDown() override
    {
        string dataFile = GET_TEST_DATA_PATH() + "modified.json";
        if (FslibWrapper::IsExist(dataFile).GetOrThrow()) {
            FslibWrapper::DeleteFileE(GET_TEMP_DATA_PATH() + "modified.json", DeleteOption::NoFence(false));
        }
    }

    void TestCaseForCompability()
    {
        /**
         * add a new_field, a new_index, a new_field to attribute and summary
         * this new config should be compatible with check_me.json
         */
        INDEXLIB_TEST_TRUE(mRawSchema.get() != NULL);
        string compatibleSchemaFile = GET_PRIVATE_TEST_DATA_PATH() + "compatible_with_check_me.json";
        IndexPartitionSchemaPtr compatibleSchema = LoadSchema(compatibleSchemaFile);
        mRawSchema->AssertCompatible(*compatibleSchema);
    }

    void TestCaseForInCompabilityWithFieldIdChanged()
    {
        string schemaFile = GET_PRIVATE_TEST_DATA_PATH() + "incompatible_with_check_me_1.json";
        InternalTestForInCompability(schemaFile);
    }

    void TestCaseForInCompabilityWithFieldTypeChanged()
    {
        string schemaFile = GET_PRIVATE_TEST_DATA_PATH() + "incompatible_with_check_me_2.json";
        InternalTestForInCompability(schemaFile);
    }

    void TestCaseForInCompabilityWithAttrIdChanged()
    {
        string schemaFile = GET_PRIVATE_TEST_DATA_PATH() + "incompatible_with_check_me_3.json";
        InternalTestForInCompability(schemaFile);
    }

    void TestCaseForInCompabilityWithSummaryIdChanged()
    {
        string schemaFile = GET_PRIVATE_TEST_DATA_PATH() + "incompatible_with_check_me_4.json";
        InternalTestForInCompability(schemaFile);
    }

    void TestCaseForCompabilityWithAddVirtualField()
    {
        string schemaFile = GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json";
        IndexPartitionSchemaPtr rawSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(rawSchema != NULL);

        size_t fieldCount = rawSchema->GetFieldCount();
        IndexPartitionSchemaPtr modifiedSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(modifiedSchema != NULL);

        FieldConfigPtr fieldConfig = modifiedSchema->AddFieldConfig("virtual", ft_long, false, true);
        INDEXLIB_TEST_TRUE(fieldConfig != NULL);
        INDEXLIB_TEST_EQUAL((fieldid_t)fieldCount, fieldConfig->GetFieldId());
        INDEXLIB_TEST_EQUAL("virtual", fieldConfig->GetFieldName());
        INDEXLIB_TEST_EQUAL(ft_long, fieldConfig->GetFieldType());
        INDEXLIB_TEST_EQUAL(true, fieldConfig->IsVirtual());
        INDEXLIB_TEST_EQUAL(fieldCount + 1, modifiedSchema->GetFieldCount());

        rawSchema->AssertCompatible(*modifiedSchema);
    }

    void TestCaseForCompabilityWithAddVirtualIndex()
    {
        string schemaFile = GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json";
        IndexPartitionSchemaPtr rawSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(rawSchema != NULL);

        size_t fieldCount = rawSchema->GetFieldCount();
        size_t indexCount = rawSchema->GetIndexSchema()->GetIndexCount();
        IndexPartitionSchemaPtr modifiedSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(modifiedSchema != NULL);

        FieldConfigPtr fieldConfig = modifiedSchema->AddFieldConfig("virtual", ft_long, false, true);
        INDEXLIB_TEST_TRUE(fieldConfig != NULL);

        IndexConfigPtr indexConfig = IndexConfigCreator::CreateSingleFieldIndexConfig(
            "virtual", it_number, "virtual", "", true, modifiedSchema->GetFieldSchema());
        modifiedSchema->AddIndexConfig(indexConfig);
        INDEXLIB_TEST_TRUE(indexConfig != NULL);

        INDEXLIB_TEST_EQUAL((fieldid_t)fieldCount, fieldConfig->GetFieldId());
        INDEXLIB_TEST_EQUAL("virtual", fieldConfig->GetFieldName());
        INDEXLIB_TEST_EQUAL(ft_long, fieldConfig->GetFieldType());
        INDEXLIB_TEST_EQUAL(true, fieldConfig->IsVirtual());
        INDEXLIB_TEST_EQUAL(fieldCount + 1, modifiedSchema->GetFieldCount());

        INDEXLIB_TEST_EQUAL((indexid_t)indexCount, indexConfig->GetIndexId());
        INDEXLIB_TEST_EQUAL(true, indexConfig->IsVirtual());
        INDEXLIB_TEST_EQUAL(indexCount + 1, modifiedSchema->GetIndexSchema()->GetIndexCount());

        rawSchema->AssertCompatible(*modifiedSchema);
    }

    void TestCaseForInCompabilityWithAddNonVirtualFieldForIndex()
    {
        string schemaFile = GET_PRIVATE_TEST_DATA_PATH() + "index_engine_example.json";
        IndexPartitionSchemaPtr rawSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(rawSchema != NULL);

        IndexPartitionSchemaPtr modifiedSchema = LoadSchema(schemaFile);
        INDEXLIB_TEST_TRUE(modifiedSchema != NULL);

        bool exceptionCaught = false;
        IndexConfigPtr indexConfig = IndexConfigCreator::CreateSingleFieldIndexConfig(
            "virtual", it_number, "user_id", "", true, modifiedSchema->GetFieldSchema());
        modifiedSchema->AddIndexConfig(indexConfig);
        try {
            IndexSchemaPtr indexSchema = modifiedSchema->GetIndexSchema();
            IndexConfigPtr indexConfig = indexSchema->GetIndexConfig("virtual");
            indexConfig->Check();

        } catch (SchemaException& e) {
            exceptionCaught = true;
        }
        INDEXLIB_TEST_TRUE(exceptionCaught);
    }

private:
    IndexPartitionSchemaPtr LoadSchema(const string& schemaPath)
    {
        ifstream in(schemaPath.c_str());
        string line;
        string jsonString;
        while (getline(in, line)) {
            jsonString += line;
        }
        Any any = ParseJson(jsonString);
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        FromJson(*schema, any);
        return schema;
    }

    void InternalTestForInCompability(const string& schemaFile)
    {
        INDEXLIB_TEST_TRUE(mRawSchema.get() != NULL);
        IndexPartitionSchemaPtr compatibleSchema = LoadSchema(schemaFile);
        const IndexPartitionSchemaImplPtr& compatibleImpl = compatibleSchema->GetImpl();
        const IndexPartitionSchemaImplPtr& rawImpl = mRawSchema->GetImpl();
        INDEXLIB_EXPECT_EXCEPTION(rawImpl->DoAssertCompatible(*(compatibleImpl.get())), AssertEqualException);
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
}} // namespace indexlib::config
