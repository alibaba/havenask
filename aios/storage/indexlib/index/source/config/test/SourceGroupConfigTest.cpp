#include "indexlib/index/source/config/SourceGroupConfig.h"

#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/config/GroupDataParameter.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {

class SourceGroupConfigTest : public TESTBASE
{
public:
    SourceGroupConfigTest() = default;
    ~SourceGroupConfigTest() = default;

private:
    void InnerCheckJsonize(SourceGroupConfig groupConfig);
};

TEST_F(SourceGroupConfigTest, TestSimpleProcess)
{
    {
        string configStr = R"(
        {
            "field_mode": "user_define",
            "module":  "testmodule",
            "parameter" : {
                "compress_type": "uniq|equal",
                "doc_compressor": "zlib",
                "file_compressor" : "snappy",
                "file_compress_buffer_size" : 8192
            }
        })";
        SourceGroupConfig groupConfig;
        Any any = ParseJson(configStr);
        ASSERT_NO_THROW(FromJson(groupConfig, any));

        ASSERT_EQ(SourceGroupConfig::SourceFieldMode::USER_DEFINE, groupConfig.GetFieldMode());
        ASSERT_EQ(string("testmodule"), groupConfig.GetModule());
        CompressTypeOption expectOption;
        ASSERT_TRUE(expectOption.Init("uniq|equal").IsOK());

        auto parameter = groupConfig.GetParameter();
        ASSERT_EQ(expectOption, parameter.GetCompressTypeOption());
        ASSERT_EQ(string("zlib"), parameter.GetDocCompressor());
        ASSERT_EQ(string("snappy"), parameter.GetFileCompressor());
        ASSERT_EQ((size_t)8192, parameter.GetFileCompressBufferSize());
        InnerCheckJsonize(groupConfig);
    }
    {
        string configStr = R"(
        {
            "field_mode": "all_field",
            "parameter" : {
                "compress_type": "uniq"
            }
        })";
        SourceGroupConfig groupConfig;
        Any any = ParseJson(configStr);
        ASSERT_NO_THROW(FromJson(groupConfig, any));

        ASSERT_EQ(SourceGroupConfig::SourceFieldMode::ALL_FIELD, groupConfig.GetFieldMode());
        ASSERT_EQ(string(""), groupConfig.GetModule());
        auto parameter = groupConfig.GetParameter();
        ASSERT_TRUE(parameter.GetCompressTypeOption().HasUniqEncodeCompress());
        InnerCheckJsonize(groupConfig);
    }
    {
        string configStr = R"(
        {
            "field_mode": "all_field"
        })";
        SourceGroupConfig groupConfig;
        Any any = ParseJson(configStr);
        ASSERT_NO_THROW(FromJson(groupConfig, any));

        CompressTypeOption expectOptions;
        auto parameter = groupConfig.GetParameter();
        ASSERT_EQ(expectOptions, parameter.GetCompressTypeOption());

        ASSERT_EQ(SourceGroupConfig::SourceFieldMode::ALL_FIELD, groupConfig.GetFieldMode());
        ASSERT_EQ(string(""), groupConfig.GetModule());
        InnerCheckJsonize(groupConfig);
    }
    {
        string configStr = R"(
        {
            "field_mode": "specified_field",
            "fields": ["ops_app_name", "ops_app_schema"]
        })";
        SourceGroupConfig groupConfig;
        Any any = ParseJson(configStr);
        ASSERT_NO_THROW(FromJson(groupConfig, any));

        ASSERT_EQ(SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD, groupConfig.GetFieldMode());
        ASSERT_EQ(vector<string>({"ops_app_name", "ops_app_schema"}), groupConfig.GetSpecifiedFields());
        InnerCheckJsonize(groupConfig);
    }
}

TEST_F(SourceGroupConfigTest, TestExceptionCase)
{
    {
        // not allow source group config without field_mode
        string configStr = R"(
        {
        })";
        SourceGroupConfig groupConfig;
        Any any = ParseJson(configStr);
        ASSERT_ANY_THROW(FromJson(groupConfig, any));
    }
    {
        // not allow specified_field with module
        string configStr = R"(
        {
            "field_mode": "specified_field",
            "fields": ["ops_app_name", "ops_app_schema"],
            "module": "a"
        })";
        SourceGroupConfig groupConfig;
        Any any = ParseJson(configStr);
        ASSERT_NO_THROW(FromJson(groupConfig, any));
        ASSERT_FALSE(groupConfig.Check().IsOK());
    }
    {
        // not allow specified_field without fields
        string configStr = R"(
        {
            "field_mode": "specified_field"
        })";
        SourceGroupConfig groupConfig;
        Any any = ParseJson(configStr);
        ASSERT_NO_THROW(FromJson(groupConfig, any));
        ASSERT_FALSE(groupConfig.Check().IsOK());
    }
    {
        // not allow all_field with module
        string configStr = R"(
        {
            "field_mode": "all_field",
            "module": "a"
        })";
        SourceGroupConfig groupConfig;
        Any any = ParseJson(configStr);
        ASSERT_NO_THROW(FromJson(groupConfig, any));
        ASSERT_FALSE(groupConfig.Check().IsOK());
    }
    {
        // not allow user_defined without module
        string configStr = R"(
        {
            "field_mode": "user_define"
        })";
        SourceGroupConfig groupConfig;
        Any any = ParseJson(configStr);
        ASSERT_NO_THROW(FromJson(groupConfig, any));
        ASSERT_FALSE(groupConfig.Check().IsOK());
    }
}

void SourceGroupConfigTest::InnerCheckJsonize(SourceGroupConfig groupConfig)
{
    Any toAny = ToJson(groupConfig);
    string jsonString = ToString(toAny);
    Any comparedAny = ParseJson(jsonString);
    SourceGroupConfig comparedConfig;
    FromJson(comparedConfig, toAny);
    ASSERT_TRUE(groupConfig.CheckEqual(comparedConfig).IsOK());
}
}} // namespace indexlib::config
