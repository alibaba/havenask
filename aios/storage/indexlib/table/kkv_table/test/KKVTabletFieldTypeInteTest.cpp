#include "fslib/fs/ExceptionTrigger.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/KKVReaderImpl.h"
#include "indexlib/table/kkv_table/KKVTabletFactory.h"
#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2::table {

class KKVTabletFieldTypeInteTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    KKVTabletFieldTypeInteTest() {}
    ~KKVTabletFieldTypeInteTest() = default;

    void CaseSetUp() override;
    void CaseTearDown() override {}
    void PrepareTableOptions(const std::string& jsonStr);
    void PrepareSchema(const std::string& filename);
    void PrepareSchema(const std::string& filename, FieldType pkeyType, FieldType skeyType, FieldType valueType);
    std::string docString(std::vector<std::string> docPrefix);
    std::string docString1();
    std::string docString2();
    std::string docString3();

private:
    void DoTestInternal();
    void DoTestPkeyType(FieldType pkeyType);
    void DoTestSkeyType(FieldType skeyType);
    void DoTestValueType(FieldType valueType);
    void DoTestValueTypeWithValueFormat(FieldType valueType);
    void DoTestValueTypeWithValueFormat(FieldType valueType, const std::string& valueFormat);

private:
    framework::IndexRoot _indexRoot;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::string _tableOptionsJsonStr;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    int64_t _offset = 0;
};

void KKVTabletFieldTypeInteTest::CaseSetUp()
{
    _offset = 0;

    _indexRoot = framework::IndexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    _tableOptionsJsonStr = R"( {
        "online_index_config": {
            "build_config": {
                "sharding_column_num" : 2,
                "level_num" : 3,
                "building_memory_limit_mb": 8,
                "need_deploy_index": false,
                "need_read_remote_index": true
            }
        }
        } )";

    PrepareTableOptions(_tableOptionsJsonStr);
}

std::string KKVTabletFieldTypeInteTest::docString(std::vector<std::string> docPrefix)
{
    std::string ret;
    for (auto prefix : docPrefix) {
        _offset++;
        ret += prefix + std::to_string(_offset) + ";";
    }
    return ret;
}

std::string KKVTabletFieldTypeInteTest::docString1()
{
    std::vector<std::string> docPrefix = {
        "cmd=add,pkey=1,skey=1,value=1,ts=1000000,locator=0:", "cmd=add,pkey=2,skey=1,value=1,ts=2000000,locator=0:",
        "cmd=add,pkey=3,skey=1,value=1,ts=3000000,locator=0:", "cmd=add,pkey=4,skey=1,value=1,ts=4000000,locator=0:",
        "cmd=delete,pkey=6,skey=1,ts=4000000,locator=0:"};
    return docString(docPrefix);
}

std::string KKVTabletFieldTypeInteTest::docString2()
{
    std::vector<std::string> docPrefix = {"cmd=add,pkey=1,skey=2,value=2,ts=5000000,locator=0:",
                                          "cmd=add,pkey=6,skey=1,value=1,ts=6000000,locator=0:",
                                          "cmd=delete,pkey=3,ts=13000000,locator=0:",
                                          "cmd=add,pkey=3,skey=2,value=2,ts=7000000,locator=0:",
                                          "cmd=add,pkey=7,skey=1,value=1,ts=8000000,locator=0:",
                                          "cmd=add,pkey=8,skey=1,value=1,ts=9000000,locator=0:",
                                          "cmd=delete,pkey=6,skey=2,ts=10000000,locator=0:",
                                          "cmd=delete,pkey=2,ts=13000000,locator=0:"};
    return docString(docPrefix);
}
std::string KKVTabletFieldTypeInteTest::docString3()
{
    std::vector<std::string> docPrefix = {
        "cmd=add,pkey=1,skey=3,value=3,ts=12000000,locator=0:", "cmd=add,pkey=6,skey=2,value=2,ts=13000000,locator=0:",
        "cmd=add,pkey=3,skey=3,value=3,ts=14000000,locator=0:", "cmd=add,pkey=7,skey=2,value=2,ts=15000000,locator=0:",
        "cmd=delete,pkey=3,skey=3,ts=16000000,locator=0:"};
    return docString(docPrefix);
}

void KKVTabletFieldTypeInteTest::PrepareTableOptions(const std::string& jsonStr)
{
    _tabletOptions = KKVTableTestHelper::CreateTableOptions(jsonStr);
    // NO-SERIALIZE OPTIONS
    _tabletOptions->SetIsOnline(true);
}

void KKVTabletFieldTypeInteTest::PrepareSchema(const std::string& filename)
{
    _schema = KKVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), filename);
    _indexConfig = KKVTableTestHelper::GetIndexConfig(_schema, "pkey");
}

void KKVTabletFieldTypeInteTest::PrepareSchema(const std::string& filename, FieldType pkeyType, FieldType skeyType,
                                               FieldType valueType)
{
    std::string pkeyTypeStr(indexlibv2::config::FieldConfig::FieldTypeToStr(pkeyType));
    std::string skeyTypeStr(indexlibv2::config::FieldConfig::FieldTypeToStr(skeyType));
    std::string valueTypeStr(indexlibv2::config::FieldConfig::FieldTypeToStr(valueType));

    string schemaPath = GET_PRIVATE_TEST_DATA_PATH() + "/" + filename;
    auto schemaJson = KKVTableTestHelper::LoadSchemaJson(schemaPath);

    // ########## SET FIELD TYPE START ###############
    using Helper = KKVTableTestHelper;
    // pkey
    ASSERT_TRUE(Helper::UpdateSchemaJsonNode(schemaJson, "fields[0].field_type", pkeyTypeStr));
    // skey
    ASSERT_TRUE(Helper::UpdateSchemaJsonNode(schemaJson, "fields[1].field_type", skeyTypeStr));
    // value
    ASSERT_TRUE(Helper::UpdateSchemaJsonNode(schemaJson, "fields[2].field_type", valueTypeStr));
    // ########## SET FIELD TYPE END ###############

    _schema = KKVTableTestHelper::LoadSchemaByJson(schemaJson);
    _indexConfig = KKVTableTestHelper::GetIndexConfig(_schema, "pkey");

    ASSERT_EQ(_indexConfig->GetPrefixFieldConfig()->GetFieldType(), pkeyType);
    ASSERT_EQ(_indexConfig->GetSuffixFieldConfig()->GetFieldType(), skeyType);
    if (_indexConfig->GetValueConfig()->GetAttributeCount() == 2) {
        ASSERT_FALSE(_indexConfig->OptimizedStoreSKey());
        ASSERT_EQ(_indexConfig->GetValueConfig()->GetAttributeConfig(0)->GetFieldType(), skeyType);
        ASSERT_EQ(_indexConfig->GetValueConfig()->GetAttributeConfig(1)->GetFieldType(), valueType);
    } else {
        ASSERT_TRUE(_indexConfig->OptimizedStoreSKey());
        ASSERT_EQ(_indexConfig->GetValueConfig()->GetAttributeConfig(0)->GetFieldType(), valueType);
    }
}

void KKVTabletFieldTypeInteTest::DoTestPkeyType(FieldType pkeyType)
{
    PrepareSchema("kkv_schema.json", pkeyType, ft_int16, ft_int16);
    DoTestInternal();
}

void KKVTabletFieldTypeInteTest::DoTestSkeyType(FieldType skeyType)
{
    {
        PrepareSchema("kkv_schema.json", ft_int64, skeyType, ft_int8);
        DoTestInternal();
    }

    {
        PrepareSchema("kkv_schema.json", ft_int64, skeyType, ft_int64);
        DoTestInternal();
    }

    {
        PrepareSchema("kkv_schema.json", ft_int64, skeyType, ft_string);
        DoTestInternal();
    }
}

void KKVTabletFieldTypeInteTest::DoTestValueType(FieldType valueType)
{
    {
        PrepareSchema("kkv_schema.json", ft_int64, ft_int8, valueType);
        DoTestInternal();
    }

    {
        PrepareSchema("kkv_schema.json", ft_int64, ft_int64, valueType);
        DoTestInternal();
    }

    {
        PrepareSchema("kkv_schema.json", ft_int64, ft_string, valueType);
        DoTestInternal();
    }
}

void KKVTabletFieldTypeInteTest::DoTestValueTypeWithValueFormat(FieldType valueType)
{
    DoTestValueTypeWithValueFormat(valueType, "plain");
    DoTestValueTypeWithValueFormat(valueType, "impact");
}

void KKVTabletFieldTypeInteTest::DoTestValueTypeWithValueFormat(FieldType valueType, const std::string& valueFormat)
{
    {
        PrepareSchema("kkv_schema.json", ft_int64, ft_int8, valueType);
        KKVTableTestHelper::SetIndexConfigValueParam(_indexConfig, valueFormat);
        DoTestInternal();
    }

    {
        PrepareSchema("kkv_schema.json", ft_int64, ft_string, valueType);
        KKVTableTestHelper::SetIndexConfigValueParam(_indexConfig, valueFormat);
        DoTestInternal();
    }
}

void KKVTabletFieldTypeInteTest::DoTestInternal()
{
    // config for merge
    _tabletOptions->SetIsLeader(true);
    _tabletOptions->SetFlushLocal(false);
    _tabletOptions->SetFlushRemote(true);

    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    ASSERT_TRUE(helper.BuildSegment(docString1()).IsOK());

    ASSERT_TRUE(helper.BuildSegment(docString2()).IsOK());

    {
        // check result
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=2,value=2;skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "6", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "7", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "8", "skey=1,value=1"));
    }
    {
        // test building segment
        ASSERT_TRUE(helper.Build(docString1()).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,value=1;skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=1,value=1;skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "6", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "7", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "8", "skey=1,value=1"));
    }
    {
        // test building & dumping segment
        ASSERT_TRUE(helper.GetTablet()->Flush().IsOK());
        ASSERT_TRUE(helper.Build(docString3()).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=3,value=3;skey=1,value=1;skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=1,value=1;skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "6", "skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "7", "skey=2,value=2;skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "8", "skey=1,value=1"));
    }

    {
        // test merged segment
        // merge will drop rt
        ASSERT_TRUE(helper.BuildSegment("cmd=add,pkey=9,skey=1,value=1,ts=17000000,locator=0:117").IsOK());
        ASSERT_TRUE(helper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
        // TODO(xinfei.sxf) optimize merge not impl, we don't know skey output order
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "6", "skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "8", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "9", "skey=1,value=1"));
    }

    {
        // test build after merge
        ASSERT_TRUE(helper.Build("cmd=add,pkey=9,skey=2,value=2,ts=17000000,locator=0:118").IsOK());
        ASSERT_TRUE(helper.Build("cmd=add,pkey=2,skey=2,value=2,ts=17000000,locator=0:119").IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=2;skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "6", "skey=2,value=2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "8", "skey=1,value=1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "9", "skey=2,value=2;skey=1,value=1"));
    }
}

TEST_F(KKVTabletFieldTypeInteTest, TestPkeyTypeInt8) { DoTestPkeyType(ft_int8); }
TEST_F(KKVTabletFieldTypeInteTest, TestPkeyTypeUInt8) { DoTestPkeyType(ft_uint8); }
TEST_F(KKVTabletFieldTypeInteTest, TestPkeyTypeInt16) { DoTestPkeyType(ft_int16); }
TEST_F(KKVTabletFieldTypeInteTest, TestPkeyTypeUInt16) { DoTestPkeyType(ft_uint16); }
TEST_F(KKVTabletFieldTypeInteTest, TestPkeyTypeInt32) { DoTestPkeyType(ft_int32); }
TEST_F(KKVTabletFieldTypeInteTest, TestPkeyTypeUInt32) { DoTestPkeyType(ft_uint32); }
TEST_F(KKVTabletFieldTypeInteTest, TestPkeyTypeInt64) { DoTestPkeyType(ft_int64); }
TEST_F(KKVTabletFieldTypeInteTest, TestPkeyTypeUInt64) { DoTestPkeyType(ft_uint64); }
TEST_F(KKVTabletFieldTypeInteTest, TestPkeyTypeString) { DoTestPkeyType(ft_string); }

TEST_F(KKVTabletFieldTypeInteTest, TestSkeyTypeInt8) { DoTestSkeyType(ft_int8); }
TEST_F(KKVTabletFieldTypeInteTest, TestSkeyTypeUInt8) { DoTestSkeyType(ft_uint8); }
TEST_F(KKVTabletFieldTypeInteTest, TestSkeyTypeInt16) { DoTestSkeyType(ft_int16); }
TEST_F(KKVTabletFieldTypeInteTest, TestSkeyTypeUInt16) { DoTestSkeyType(ft_uint16); }
TEST_F(KKVTabletFieldTypeInteTest, TestSkeyTypeInt32) { DoTestSkeyType(ft_int32); }
TEST_F(KKVTabletFieldTypeInteTest, TestSkeyTypeUInt32) { DoTestSkeyType(ft_uint32); }
TEST_F(KKVTabletFieldTypeInteTest, TestSkeyTypeInt64) { DoTestSkeyType(ft_int64); }
TEST_F(KKVTabletFieldTypeInteTest, TestSkeyTypeUInt64) { DoTestSkeyType(ft_uint64); }
TEST_F(KKVTabletFieldTypeInteTest, TestSkeyTypeString) { DoTestSkeyType(ft_string); }

TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeInt8) { DoTestValueType(ft_int8); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeUInt8) { DoTestValueType(ft_uint8); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeInt16) { DoTestValueType(ft_int16); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeUInt16) { DoTestValueType(ft_uint16); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeInt32) { DoTestValueType(ft_int32); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeUInt32) { DoTestValueType(ft_uint32); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeInt64) { DoTestValueType(ft_int64); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeUInt64) { DoTestValueType(ft_uint64); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeString) { DoTestValueType(ft_string); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeWithValueFormatInt8) { DoTestValueTypeWithValueFormat(ft_int8); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeWithValueFormatUInt8) { DoTestValueTypeWithValueFormat(ft_uint8); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeWithValueFormatInt16) { DoTestValueTypeWithValueFormat(ft_int16); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeWithValueFormatUInt16) { DoTestValueTypeWithValueFormat(ft_uint16); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeWithValueFormatInt32) { DoTestValueTypeWithValueFormat(ft_int32); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeWithValueFormatUInt32) { DoTestValueTypeWithValueFormat(ft_uint32); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeWithValueFormatInt64) { DoTestValueTypeWithValueFormat(ft_int64); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeWithValueFormatUInt64) { DoTestValueTypeWithValueFormat(ft_uint64); }
TEST_F(KKVTabletFieldTypeInteTest, TestValueTypeWithValueFormatString) { DoTestValueType(ft_string); }

} // namespace indexlibv2::table
