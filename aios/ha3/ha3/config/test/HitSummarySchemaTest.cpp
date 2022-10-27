#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/HitSummarySchema.h>
#include <ha3/test/test.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>

using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);

class HitSummarySchemaTest : public TESTBASE {
public:
    HitSummarySchemaTest();
    ~HitSummarySchemaTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, HitSummarySchemaTest);


#define SUMMARY_TABLE_TEST_HELPER(name, field_type, is_multi_value, summary_field_id) \
    {                                                                   \
        const SummaryFieldInfo *fieldInfo = summaryTable.getSummaryFieldInfo(name); \
        ASSERT_TRUE(fieldInfo);                                      \
        ASSERT_EQ(FieldType(field_type), fieldInfo->fieldType); \
        ASSERT_EQ(std::string(name), fieldInfo->fieldName);  \
        ASSERT_EQ(bool(is_multi_value), fieldInfo->isMultiValue); \
        ASSERT_EQ(summaryfieldid_t(summary_field_id), fieldInfo->summaryFieldId); \
    }

HitSummarySchemaTest::HitSummarySchemaTest() {
}

HitSummarySchemaTest::~HitSummarySchemaTest() {
}

void HitSummarySchemaTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void HitSummarySchemaTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(HitSummarySchemaTest, testInit) {
    HA3_LOG(DEBUG, "Begin Test!");

    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/sample_schema.json");

    HitSummarySchema summaryTable(tableInfoPtr.get());

    ASSERT_EQ(size_t(6), summaryTable.getFieldCount());

    {
        const SummaryFieldInfo *fieldInfo = summaryTable.getSummaryFieldInfo("not_exist_field");
        ASSERT_TRUE(!fieldInfo);
    }

    SUMMARY_TABLE_TEST_HELPER("multi_int32_value", ft_integer, true, 0);
    SUMMARY_TABLE_TEST_HELPER("id", ft_integer, false, 1);
    SUMMARY_TABLE_TEST_HELPER("company_id", ft_integer, false, 2);
    SUMMARY_TABLE_TEST_HELPER("subject", ft_text, false, 3);
    SUMMARY_TABLE_TEST_HELPER("cat_id", ft_integer, false, 4);
    SUMMARY_TABLE_TEST_HELPER("keywords", ft_string, false, 5);
    ASSERT_FALSE(summaryTable._needCalcSignature);
    ASSERT_EQ(size_t(6), (size_t)summaryTable._signature & 0xff);
}

TEST_F(HitSummarySchemaTest, testAddSummaryField) {
    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/sample_schema.json");

    HitSummarySchema summaryTable(tableInfoPtr.get());

    ASSERT_EQ(size_t(6), summaryTable.getFieldCount());
    ASSERT_EQ(INVALID_SUMMARYFIELDID, summaryTable.declareSummaryField("id", ft_integer, true));
    ASSERT_EQ(summaryfieldid_t(6), summaryTable.declareSummaryField("new_field", ft_string, true));

    SUMMARY_TABLE_TEST_HELPER("new_field", ft_string, true, 6);
}

TEST_F(HitSummarySchemaTest, testGetSignature) {
    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/sample_schema.json");

    HitSummarySchema summaryTable(tableInfoPtr.get());
    int64_t hash1 = summaryTable.getSignature();
    ASSERT_EQ(size_t(6), (size_t)hash1 & 0xff);

    ASSERT_EQ(summaryfieldid_t(6), summaryTable.declareSummaryField("new_field1", ft_string, true));
    ASSERT_EQ(summaryfieldid_t(7), summaryTable.declareSummaryField("new_field2", ft_text, false));

    int64_t hash2 = summaryTable.getSignature();
    ASSERT_EQ(size_t(8), (size_t)hash2 & 0xff);

    ASSERT_TRUE(hash1 != hash2);
}

TEST_F(HitSummarySchemaTest, testSerialize) {
    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/sample_schema.json");

    HitSummarySchema oldSummaryTable(tableInfoPtr.get());
    int64_t oldHash = oldSummaryTable.getSignature();

    autil::DataBuffer dataBuffer;
    oldSummaryTable.serialize(dataBuffer);

    HitSummarySchema summaryTable;
    summaryTable.deserialize(dataBuffer);

    SUMMARY_TABLE_TEST_HELPER("multi_int32_value", ft_integer, true, 0);
    SUMMARY_TABLE_TEST_HELPER("id", ft_integer, false, 1);
    SUMMARY_TABLE_TEST_HELPER("company_id", ft_integer, false, 2);
    SUMMARY_TABLE_TEST_HELPER("subject", ft_text, false, 3);
    SUMMARY_TABLE_TEST_HELPER("cat_id", ft_integer, false, 4);
    SUMMARY_TABLE_TEST_HELPER("keywords", ft_string, false, 5);

    ASSERT_EQ(oldHash, summaryTable.getSignature());
}

TEST_F(HitSummarySchemaTest, testSerializeWithInc) {
    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/sample_schema.json");

    HitSummarySchema oldSummaryTable(tableInfoPtr.get());
    ASSERT_EQ(summaryfieldid_t(6), oldSummaryTable.declareSummaryField("new_field1", ft_string, true));
    int64_t oldHash = oldSummaryTable.getSignature();

    autil::DataBuffer dataBuffer;
    oldSummaryTable.serialize(dataBuffer);

    HitSummarySchema summaryTable;
    summaryTable.deserialize(dataBuffer);
    ASSERT_EQ(oldHash, summaryTable.getSignature());
    SUMMARY_TABLE_TEST_HELPER("multi_int32_value", ft_integer, true, 0);
    SUMMARY_TABLE_TEST_HELPER("id", ft_integer, false, 1);
    SUMMARY_TABLE_TEST_HELPER("company_id", ft_integer, false, 2);
    SUMMARY_TABLE_TEST_HELPER("subject", ft_text, false, 3);
    SUMMARY_TABLE_TEST_HELPER("cat_id", ft_integer, false, 4);
    SUMMARY_TABLE_TEST_HELPER("keywords", ft_string, false, 5);
    SUMMARY_TABLE_TEST_HELPER("new_field1", ft_string, true, 6);
}

TEST_F(HitSummarySchemaTest, testGetSignatureNoCalc) {
    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/sample_schema.json");

    HitSummarySchema summaryTable(tableInfoPtr.get());
    ASSERT_FALSE(summaryTable._needCalcSignature);
    int64_t hash1 = summaryTable.getSignatureNoCalc();
    ASSERT_EQ(size_t(6), (size_t)hash1 & 0xff);

    ASSERT_EQ(summaryfieldid_t(6), summaryTable.declareSummaryField("new_field1", ft_string, true));
    ASSERT_EQ(summaryfieldid_t(7), summaryTable.declareSummaryField("new_field2", ft_text, false));

    ASSERT_EQ(hash1, summaryTable.getSignatureNoCalc());
    int64_t hash2 = summaryTable.getSignature();
    int64_t hash3 = summaryTable.getSignatureNoCalc();
    ASSERT_EQ(hash2, hash3);
    ASSERT_EQ(size_t(8), (size_t)hash2 & 0xff);
    ASSERT_TRUE(hash1 != hash2);
}

TEST_F(HitSummarySchemaTest, testHitSummarySchemaPool) {
    TableInfoPtr tableInfoPtr = TableInfoConfigurator::createFromFile(TEST_DATA_PATH"/config_test/sample_schema.json");
    HitSummarySchemaPtr base(new HitSummarySchema(tableInfoPtr.get()));
    ASSERT_EQ(size_t(6), base->getFieldCount());
    HitSummarySchemaPool schemaPool(base, 1);
    ASSERT_EQ(1, schemaPool._schemaVec.size());
    HitSummarySchemaPtr schema = schemaPool.get();
    ASSERT_TRUE(schema != NULL);
    ASSERT_EQ(0, schemaPool._schemaVec.size());
    ASSERT_EQ(size_t(6), schema->getFieldCount());
    ASSERT_EQ(summaryfieldid_t(6), schema->declareSummaryField("new_field1", ft_string, true));
    ASSERT_TRUE(schema->getSummaryFieldInfo("new_field1") != NULL);
    ASSERT_EQ(size_t(7), schema->getFieldCount());

    HitSummarySchemaPtr schema2 = schemaPool.get();
    ASSERT_TRUE(schema2 != NULL);
    ASSERT_EQ(0, schemaPool._schemaVec.size());
    ASSERT_EQ(size_t(6), schema2->getFieldCount());

    schemaPool.put(schema);
    ASSERT_EQ(1, schemaPool._schemaVec.size());

    schema = schemaPool.get();
    ASSERT_TRUE(schema != NULL);
    ASSERT_EQ(0, schemaPool._schemaVec.size());
    ASSERT_TRUE(schema->getSummaryFieldInfo("new_field1") == NULL);

    ASSERT_EQ(size_t(6), schema->getFieldCount());
}

#undef SUMMARY_TABLE_TEST_HELPER

END_HA3_NAMESPACE(config);

