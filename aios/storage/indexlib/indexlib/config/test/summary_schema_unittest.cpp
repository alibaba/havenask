#include "indexlib/config/test/summary_schema_unittest.h"

#include <fstream>

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;

using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace config {
IE_LOG_SETUP(config, SummarySchemaTest);

SummarySchemaTest::SummarySchemaTest() {}

SummarySchemaTest::~SummarySchemaTest() {}

void SummarySchemaTest::CaseSetUp()
{
    mRootDir = GET_PRIVATE_TEST_DATA_PATH();
    mJsonStringHead = R"(
    {
        "table_name": "mainse_summary",
        "table_type": "normal",
        "attributes": [ "quantity", "provcity", "vip" ],
        "fields": [
            { "field_name": "quantity", "field_type": "INTEGER" },
            { "field_name": "provcity", "compress_type": "uniq|equal", "field_type": "STRING" },
            { "field_name": "category", "field_type": "INTEGER" },
            { "field_name": "nid", "field_type": "STRING" },
            { "field_name": "zk_time", "field_type": "STRING" },
            { "field_name": "title", "field_type": "STRING" },
            { "field_name": "user", "field_type": "STRING" },
            { "field_name": "user_id", "field_type": "STRING" },
            { "field_name": "vip", "field_type": "STRING" },
            { "field_name": "ends", "field_type": "STRING" },
            { "field_name": "pid", "field_type": "STRING" },
            { "field_name": "nick", "field_type": "STRING" }
        ],
        "indexs": [
            {
                "index_fields": "nid",
                "index_name": "pk",
                "index_type": "PRIMARYKEY64",
                "pk_hash_type": "default_hash",
                "pk_storage_type": "hash_table"
            }
        ],
    )";
    mJsonStringTail = R"(
    }
    )";
}

void SummarySchemaTest::CaseTearDown() {}

void SummarySchemaTest::TestSimpleProcess()
{
    string jsonString;
    string confFile = GET_PRIVATE_TEST_DATA_PATH() + "summary_group_schema.json";
    FslibWrapper::AtomicLoadE(confFile, jsonString);
    Any any = ParseJson(jsonString);

    string jsonString1 = ToString(any);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
    FromJson(*schema, any);

    Any any2 = ToJson(*schema);
    string jsonString2 = ToString(any2);
    Any comparedAny = ParseJson(jsonString2);
    IndexPartitionSchemaPtr comparedSchema(new IndexPartitionSchema("noname"));
    FromJson(*comparedSchema, comparedAny);
    ASSERT_NO_THROW(schema->AssertEqual(*comparedSchema));
}

void SummarySchemaTest::TestSummaryGroup()
{
    string jsonString;
    FslibWrapper::AtomicLoadE(mRootDir + "summary_group_schema.json", jsonString);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJsonString(*schema, jsonString);
    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();

    // SummarySchema
    ASSERT_EQ(4, summarySchema->GetSummaryGroupConfigCount());
    ASSERT_TRUE(summarySchema->NeedStoreSummary());
    vector<string> fields = {"nid",     "title", "pid",  "provcity", "category", "quantity",
                             "zk_time", "ends",  "user", "user_id",  "nick",     "vip"};
    ASSERT_EQ(fields.size(), summarySchema->GetSummaryCount());
    SummarySchema::Iterator sit = summarySchema->Begin();
    for (size_t i = 0; i < fields.size(); ++i, ++sit) {
        ASSERT_EQ(fields[i], (*sit)->GetSummaryName());
        fieldid_t fieldId = schema->GetFieldId(fields[i]);
        ASSERT_TRUE(summarySchema->IsInSummary(fieldId));
        ASSERT_EQ(i, summarySchema->GetSummaryFieldId(fieldId));
        ASSERT_EQ(fields[i], summarySchema->GetSummaryConfig(fieldId)->GetSummaryName());
    }

    // group 0 (default): compress, has fields in attributes
    SummaryGroupConfigPtr defaultGroupConfig = summarySchema->GetSummaryGroupConfig(DEFAULT_SUMMARYGROUPNAME);
    ASSERT_EQ(0, summarySchema->GetSummaryGroupId(DEFAULT_SUMMARYGROUPNAME));
    ASSERT_TRUE(defaultGroupConfig == summarySchema->GetSummaryGroupConfig(DEFAULT_SUMMARYGROUPID));
    ASSERT_EQ(DEFAULT_SUMMARYGROUPNAME, defaultGroupConfig->GetGroupName());
    ASSERT_TRUE(defaultGroupConfig->IsCompress());
    ASSERT_TRUE(defaultGroupConfig->NeedStoreSummary());
    ASSERT_EQ(0, defaultGroupConfig->GetSummaryFieldIdBase());
    fields = {"nid", "title", "pid", "provcity", "category"};
    ASSERT_EQ(fields.size(), defaultGroupConfig->GetSummaryFieldsCount());
    SummaryGroupConfig::Iterator it = defaultGroupConfig->Begin();
    for (size_t i = 0; it != defaultGroupConfig->End(); ++it, ++i) {
        ASSERT_EQ(fields[i], (*it)->GetSummaryName());
        ASSERT_EQ(0, summarySchema->FieldIdToSummaryGroupId(summarySchema->GetSummaryConfig(fields[i])->GetFieldId()));
    }

    // group 1: compress, one field in attributes
    SummaryGroupConfigPtr group = summarySchema->GetSummaryGroupConfig(1);
    ASSERT_EQ(1, summarySchema->GetSummaryGroupId("mainse"));
    ASSERT_TRUE(group == summarySchema->GetSummaryGroupConfig("mainse"));
    ASSERT_EQ("mainse", group->GetGroupName());
    ASSERT_TRUE(group->IsCompress());
    ASSERT_TRUE(group->NeedStoreSummary());
    ASSERT_EQ(5, group->GetSummaryFieldIdBase());
    fields = {"quantity", "zk_time", "ends"};
    ASSERT_EQ(fields.size(), group->GetSummaryFieldsCount());
    it = group->Begin();
    for (size_t i = 0; it != group->End(); ++it, ++i) {
        ASSERT_EQ(fields[i], (*it)->GetSummaryName());
        ASSERT_EQ(1, summarySchema->FieldIdToSummaryGroupId(summarySchema->GetSummaryConfig(fields[i])->GetFieldId()));
    }

    // group 2: no compress, no field in attributes
    group = summarySchema->GetSummaryGroupConfig(2);
    ASSERT_EQ(2, summarySchema->GetSummaryGroupId("inshop"));
    ASSERT_TRUE(group == summarySchema->GetSummaryGroupConfig("inshop"));
    ASSERT_EQ("inshop", group->GetGroupName());
    ASSERT_FALSE(group->IsCompress());
    ASSERT_TRUE(group->NeedStoreSummary());
    ASSERT_EQ(8, group->GetSummaryFieldIdBase());
    fields = {"user", "user_id", "nick"};
    ASSERT_EQ(fields.size(), group->GetSummaryFieldsCount());
    it = group->Begin();
    for (size_t i = 0; it != group->End(); ++it, ++i) {
        ASSERT_EQ(fields[i], (*it)->GetSummaryName());
        ASSERT_EQ(2, summarySchema->FieldIdToSummaryGroupId(summarySchema->GetSummaryConfig(fields[i])->GetFieldId()));
    }

    // group 3: default no compress, all fields in attributes
    group = summarySchema->GetSummaryGroupConfig(3);
    ASSERT_EQ(3, summarySchema->GetSummaryGroupId("vip"));
    ASSERT_TRUE(group == summarySchema->GetSummaryGroupConfig("vip"));
    ASSERT_EQ("vip", group->GetGroupName());
    ASSERT_FALSE(group->IsCompress());
    ASSERT_FALSE(group->NeedStoreSummary());
    ASSERT_EQ(11, group->GetSummaryFieldIdBase());
    fields = {"vip"};
    ASSERT_EQ(fields.size(), group->GetSummaryFieldsCount());
    it = group->Begin();
    for (size_t i = 0; it != group->End(); ++it, ++i) {
        ASSERT_EQ(fields[i], (*it)->GetSummaryName());
        ASSERT_EQ(3, summarySchema->FieldIdToSummaryGroupId(summarySchema->GetSummaryConfig(fields[i])->GetFieldId()));
    }
}

void SummarySchemaTest::TestSummaryCompressor()
{
    {
        string jsonString;
        FslibWrapper::AtomicLoadE(mRootDir + "summary_group_schema.json", jsonString);
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
        FromJsonString(*schema, jsonString);
        const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();

        // SummarySchema
        ASSERT_EQ(4, summarySchema->GetSummaryGroupConfigCount());
        ASSERT_TRUE(summarySchema->GetSummaryGroupConfig(0)->IsCompress());
        ASSERT_TRUE(summarySchema->GetSummaryGroupConfig(1)->IsCompress());
        ASSERT_FALSE(summarySchema->GetSummaryGroupConfig(2)->IsCompress());
        ASSERT_FALSE(summarySchema->GetSummaryGroupConfig(3)->IsCompress());

        ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(0)->GetCompressType());
        ASSERT_EQ("zstd", summarySchema->GetSummaryGroupConfig(1)->GetCompressType());
        ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(2)->GetCompressType());
        ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(3)->GetCompressType());
    }
    {
        string jsonString = mJsonStringHead + R"(
        "summarys": {
            "compress": true,
            "compress_type" : "zlib",
            "summary_fields": [ "nid", "title", "pid", "provcity", "category" ],
            "summary_groups" : [
                {
                    "group_name" : "mainse",
                    "compress": false,
                    "compress_type" : "zstd",
                    "summary_fields" : [ "quantity", "zk_time", "ends" ]
                },
                {
                    "group_name" : "inshop",
                    "compress": true,
                    "summary_fields" : [ "user", "user_id", "nick" ]
                }
            ]
        }
        )" + mJsonStringTail;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_NO_THROW(FromJsonString(*schema, jsonString));
        const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();
        ASSERT_EQ(3, summarySchema->GetSummaryGroupConfigCount());
        ASSERT_TRUE(summarySchema->GetSummaryGroupConfig(0)->IsCompress());
        ASSERT_FALSE(summarySchema->GetSummaryGroupConfig(1)->IsCompress());
        ASSERT_TRUE(summarySchema->GetSummaryGroupConfig(2)->IsCompress());

        ASSERT_EQ("zlib", summarySchema->GetSummaryGroupConfig(0)->GetCompressType());
        ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(1)->GetCompressType());
        ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(2)->GetCompressType());
    }
}

void SummarySchemaTest::TestSummaryGroupWithParameter()
{
    string jsonString;
    ASSERT_EQ(FSEC_OK,
              FslibWrapper::AtomicLoad(mRootDir + "summary_group_schema_with_parameter.json", jsonString).Code());
    IndexPartitionSchemaPtr jsonSchema(new IndexPartitionSchema(""));
    FromJsonString(*jsonSchema, jsonString);

    string tmpStr = ToJsonString(jsonSchema);
    IndexPartitionSchemaPtr schema(new IndexPartitionSchema(""));
    FromJsonString(*schema, tmpStr);

    ASSERT_NO_THROW(schema->AssertEqual(*jsonSchema));

    const SummarySchemaPtr& summarySchema = schema->GetSummarySchema();

    // SummarySchema
    ASSERT_EQ(4, summarySchema->GetSummaryGroupConfigCount());
    ASSERT_TRUE(summarySchema->GetSummaryGroupConfig(0)->IsCompress());
    ASSERT_TRUE(summarySchema->GetSummaryGroupConfig(1)->IsCompress());
    ASSERT_TRUE(summarySchema->GetSummaryGroupConfig(2)->IsCompress());
    ASSERT_FALSE(summarySchema->GetSummaryGroupConfig(3)->IsCompress());

    ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(0)->GetCompressType());
    ASSERT_EQ("zstd", summarySchema->GetSummaryGroupConfig(1)->GetCompressType());
    ASSERT_EQ("zstd", summarySchema->GetSummaryGroupConfig(2)->GetCompressType());
    ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(3)->GetCompressType());

    ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(0)->GetCompressType());
    ASSERT_EQ("zstd", summarySchema->GetSummaryGroupConfig(1)->GetCompressType());
    ASSERT_EQ("zstd", summarySchema->GetSummaryGroupConfig(2)->GetCompressType());
    ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(3)->GetCompressType());

    ASSERT_TRUE(summarySchema->GetSummaryGroupConfig(0)->HasEnableAdaptiveOffset());
    ASSERT_FALSE(summarySchema->GetSummaryGroupConfig(1)->HasEnableAdaptiveOffset());
    ASSERT_TRUE(summarySchema->GetSummaryGroupConfig(2)->HasEnableAdaptiveOffset());
    ASSERT_FALSE(summarySchema->GetSummaryGroupConfig(3)->HasEnableAdaptiveOffset());

    auto fileCompressConfig =
        summarySchema->GetSummaryGroupConfig(0)->GetSummaryGroupDataParam().GetFileCompressConfig();
    ASSERT_TRUE(fileCompressConfig);
    ASSERT_EQ(string("compress1"), fileCompressConfig->GetCompressName());
    ASSERT_EQ("zstd", summarySchema->GetSummaryGroupConfig(0)->GetSummaryGroupDataParam().GetFileCompressor());

    fileCompressConfig = summarySchema->GetSummaryGroupConfig(1)->GetSummaryGroupDataParam().GetFileCompressConfig();
    ASSERT_FALSE(fileCompressConfig);
    ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(1)->GetSummaryGroupDataParam().GetFileCompressor());

    fileCompressConfig = summarySchema->GetSummaryGroupConfig(2)->GetSummaryGroupDataParam().GetFileCompressConfig();
    ASSERT_FALSE(fileCompressConfig);
    ASSERT_EQ("", summarySchema->GetSummaryGroupConfig(2)->GetSummaryGroupDataParam().GetFileCompressor());

    fileCompressConfig = summarySchema->GetSummaryGroupConfig(3)->GetSummaryGroupDataParam().GetFileCompressConfig();
    ASSERT_TRUE(fileCompressConfig);
    ASSERT_EQ(string("compress2"), fileCompressConfig->GetCompressName());
    ASSERT_EQ("snappy", summarySchema->GetSummaryGroupConfig(3)->GetSummaryGroupDataParam().GetFileCompressor());
}

void SummarySchemaTest::TestSummaryException()
{
    // default group no fields
    {
        string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": []
        }
        )" + mJsonStringTail;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_NO_THROW(FromJsonString(*schema, jsonString));
        ASSERT_FALSE(schema->GetSummarySchema());
    }

    // group no name
    {
        string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": [ "nid", "title", "pid", "provcity", "category" ],
            "summary_groups" : [
                { "summary_fields": [ "vip"] }
            ]
        }
        )" + mJsonStringTail;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_THROW(FromJsonString(*schema, jsonString), SchemaException);
    }
    // group no fields
    {
        string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": [ "nid", "title", "pid", "provcity", "category" ],
            "summary_groups" : [
                { "group_name" : "vip" }
            ]
        }
        )" + mJsonStringTail;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_THROW(FromJsonString(*schema, jsonString), SchemaException);
    }

    // group has empty fields
    {
        string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": [ "nid", "title", "pid", "provcity", "category" ],
            "summary_groups" : [
                { "group_name" : "vip", "summary_fields": [] }
            ]
        }
        )" + mJsonStringTail;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_THROW(FromJsonString(*schema, jsonString), SchemaException);
    }

    // group duplicate name
    {
        string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": [ "nid", "title", "pid", "provcity", "category" ],
            "summary_groups" : [
                { "group_name" : "vip", "summary_fields": [ "vip" ]},
                { "group_name" : "vip", "summary_fields": [ "nick" ]}
            ]
        }
        )" + mJsonStringTail;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_THROW(FromJsonString(*schema, jsonString), SchemaException);
    }

    // duplicate summary fields between dufault and group
    {
        string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": [ "nid", "title", "pid", "provcity", "category" ],
            "summary_groups" : [
                { "group_name" : "vip", "summary_fields": [ "nid" ]}
            ]
        }
        )" + mJsonStringTail;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_THROW(FromJsonString(*schema, jsonString), SchemaException);
    }
    // duplicate summary fields between groups
    {
        string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": [ "nid", "title", "pid", "provcity", "category" ],
            "summary_groups" : [
                { "group_name" : "vip", "summary_fields": [ "vip" ]},
                { "group_name" : "nick", "summary_fields": [ "vip" ]}
            ]
        }
        )" + mJsonStringTail;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_THROW(FromJsonString(*schema, jsonString), SchemaException);
    }

    // all fields in attributes
    {
        string jsonString = mJsonStringHead + R"(
        "summarys": {
            "summary_fields": [ "provcity"],
            "summary_groups" : [
                { "group_name" : "vip", "summary_fields": [ "vip" ]}
            ]
        }
        )" + mJsonStringTail;
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        ASSERT_NO_THROW(FromJsonString(*schema, jsonString));
        ASSERT_FALSE(schema->GetSummarySchema()->NeedStoreSummary());
    }
}

void SummarySchemaTest::TestJsonize()
{
    string jsonString1;
    FslibWrapper::AtomicLoadE(mRootDir + "summary_group_schema.json", jsonString1);
    IndexPartitionSchemaPtr schema1(new IndexPartitionSchema(""));
    FromJsonString(*schema1, jsonString1);
    string jsonString2 = ToJsonString(schema1);
    IndexPartitionSchemaPtr schema2(new IndexPartitionSchema(""));
    FromJsonString(*schema2, jsonString2);
    const SummarySchemaPtr& summarySchema = schema2->GetSummarySchema();

    // SummarySchema
    ASSERT_EQ(4, summarySchema->GetSummaryGroupConfigCount());
    ASSERT_TRUE(summarySchema->NeedStoreSummary());
    vector<string> fields = {"nid",     "title", "pid",  "provcity", "category", "quantity",
                             "zk_time", "ends",  "user", "user_id",  "nick",     "vip"};
    ASSERT_EQ(fields.size(), summarySchema->GetSummaryCount());
    SummarySchema::Iterator sit = summarySchema->Begin();
    for (size_t i = 0; i < fields.size(); ++i, ++sit) {
        ASSERT_EQ(fields[i], (*sit)->GetSummaryName());
        fieldid_t fieldId = schema2->GetFieldId(fields[i]);
        ASSERT_TRUE(summarySchema->IsInSummary(fieldId));
        ASSERT_EQ(i, summarySchema->GetSummaryFieldId(fieldId));
        ASSERT_EQ(fields[i], summarySchema->GetSummaryConfig(fieldId)->GetSummaryName());
    }

    // group 0 (default): compress, has fields in attributes
    SummaryGroupConfigPtr defaultGroupConfig = summarySchema->GetSummaryGroupConfig(DEFAULT_SUMMARYGROUPNAME);
    ASSERT_EQ(0, summarySchema->GetSummaryGroupId(DEFAULT_SUMMARYGROUPNAME));
    ASSERT_TRUE(defaultGroupConfig == summarySchema->GetSummaryGroupConfig(DEFAULT_SUMMARYGROUPID));
    ASSERT_EQ(DEFAULT_SUMMARYGROUPNAME, defaultGroupConfig->GetGroupName());
    ASSERT_TRUE(defaultGroupConfig->IsCompress());
    ASSERT_TRUE(defaultGroupConfig->NeedStoreSummary());
    ASSERT_EQ(0, defaultGroupConfig->GetSummaryFieldIdBase());
    fields = {"nid", "title", "pid", "provcity", "category"};
    ASSERT_EQ(fields.size(), defaultGroupConfig->GetSummaryFieldsCount());
    SummaryGroupConfig::Iterator it = defaultGroupConfig->Begin();
    for (size_t i = 0; it != defaultGroupConfig->End(); ++it, ++i) {
        ASSERT_EQ(fields[i], (*it)->GetSummaryName());
        ASSERT_EQ(0, summarySchema->FieldIdToSummaryGroupId(summarySchema->GetSummaryConfig(fields[i])->GetFieldId()));
    }

    // group 1: compress, one field in attributes
    SummaryGroupConfigPtr group = summarySchema->GetSummaryGroupConfig(1);
    ASSERT_EQ(1, summarySchema->GetSummaryGroupId("mainse"));
    ASSERT_TRUE(group == summarySchema->GetSummaryGroupConfig("mainse"));
    ASSERT_EQ("mainse", group->GetGroupName());
    ASSERT_TRUE(group->IsCompress());
    ASSERT_TRUE(group->NeedStoreSummary());
    ASSERT_EQ(5, group->GetSummaryFieldIdBase());
    fields = {"quantity", "zk_time", "ends"};
    ASSERT_EQ(fields.size(), group->GetSummaryFieldsCount());
    it = group->Begin();
    for (size_t i = 0; it != group->End(); ++it, ++i) {
        ASSERT_EQ(fields[i], (*it)->GetSummaryName());
        ASSERT_EQ(1, summarySchema->FieldIdToSummaryGroupId(summarySchema->GetSummaryConfig(fields[i])->GetFieldId()));
    }

    // group 2: no compress, no field in attributes
    group = summarySchema->GetSummaryGroupConfig(2);
    ASSERT_EQ(2, summarySchema->GetSummaryGroupId("inshop"));
    ASSERT_TRUE(group == summarySchema->GetSummaryGroupConfig("inshop"));
    ASSERT_EQ("inshop", group->GetGroupName());
    ASSERT_FALSE(group->IsCompress());
    ASSERT_TRUE(group->NeedStoreSummary());
    ASSERT_EQ(8, group->GetSummaryFieldIdBase());
    fields = {"user", "user_id", "nick"};
    ASSERT_EQ(fields.size(), group->GetSummaryFieldsCount());
    it = group->Begin();
    for (size_t i = 0; it != group->End(); ++it, ++i) {
        ASSERT_EQ(fields[i], (*it)->GetSummaryName());
        ASSERT_EQ(2, summarySchema->FieldIdToSummaryGroupId(summarySchema->GetSummaryConfig(fields[i])->GetFieldId()));
    }
}
}} // namespace indexlib::config
