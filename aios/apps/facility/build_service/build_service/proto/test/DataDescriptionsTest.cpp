#include "build_service/proto/DataDescriptions.h"

#include <iostream>
#include <map>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/ParserConfig.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace proto {

class DataDescriptionsTest : public BUILD_SERVICE_TESTBASE
{
};

TEST_F(DataDescriptionsTest, testSimple)
{
    DataDescription d1;
    d1["src"] = "file";
    d1["type"] = "file";
    DataDescription d2;
    d2["src"] = "swift";
    d2["type"] = "swift";

    DataDescriptions dataDescriptions;
    EXPECT_TRUE(dataDescriptions.empty());
    EXPECT_EQ(size_t(0), dataDescriptions.size());
    EXPECT_TRUE(dataDescriptions.validate());

    dataDescriptions.push_back(d1);
    EXPECT_FALSE(dataDescriptions.empty());
    dataDescriptions.push_back(d2);
    EXPECT_EQ(size_t(2), dataDescriptions.size());
    EXPECT_TRUE(dataDescriptions.validate());

    DataDescription d3;
    d3["src"] = "swift";
    d3["type"] = "file";
    dataDescriptions.push_back(d3);
    EXPECT_FALSE(dataDescriptions.validate());
    dataDescriptions.pop_back();

    DataDescription d5;
    dataDescriptions.push_back(d5);
    EXPECT_FALSE(dataDescriptions.validate());
    dataDescriptions.pop_back();

    DataDescription d4;
    d4["src"] = "hbase";
    d4["type"] = "hbase";
    dataDescriptions.push_back(d4);
    EXPECT_TRUE(dataDescriptions.validate());
}

TEST_F(DataDescriptionsTest, testJson)
{
    DataDescriptions dataDescriptions;
    autil::legacy::FromJsonString(dataDescriptions.toVector(), "[{\"src\":\"swift\"}]");
    EXPECT_EQ(size_t(1), dataDescriptions.size());
    EXPECT_EQ("swift", dataDescriptions[0]["src"]);
    EXPECT_EQ("", dataDescriptions[0]["type"]);
}

TEST_F(DataDescriptionsTest, testJsonizeCompatibleWithLagacyFormat)
{
    DataDescription ds;
    string jsonStr = R"(
            {
                "src" : "f1",
                "type" : "f2", 
                "data" : "f3",
                "parser_config" : [
                ]
            }
        )";
    //"""
    autil::legacy::FromJsonString(ds, jsonStr);
    string legacyJsonStr = autil::legacy::ToJsonString(ds);
    KeyValueMap kvMap;
    autil::legacy::FromJsonString(kvMap, legacyJsonStr);
    ASSERT_EQ(3u, kvMap.size());
    ASSERT_EQ(string("f1"), kvMap.at("src"));
    ASSERT_EQ(string("f2"), kvMap.at("type"));
    ASSERT_EQ(string("f3"), kvMap.at("data"));
}

TEST_F(DataDescriptionsTest, testCustomizeProcessorCount)
{
    {
        DataDescriptions dataDescriptions;
        string jsonStr = R"(
        [
            {
                "src" : "file",
                "type" : "0",
                "full_processor_count" : "100"
            },
            {
                "src" : "odps",
                "type" : "1",
                "full_processor_count" : "101"
            },
            {
                "src" : "swift",
                "type" : "2",
                "full_processor_count" : "102"
            }
        ]
        )";
        autil::legacy::FromJsonString(dataDescriptions.toVector(), jsonStr);
        ASSERT_TRUE(dataDescriptions.validate());
        EXPECT_EQ(3u, dataDescriptions.size());
        EXPECT_EQ("file", dataDescriptions[0]["src"]);
        EXPECT_EQ("odps", dataDescriptions[1]["src"]);
        EXPECT_EQ("swift", dataDescriptions[2]["src"]);

        EXPECT_EQ("100", dataDescriptions[0]["full_processor_count"]);
        EXPECT_EQ("101", dataDescriptions[1]["full_processor_count"]);
        EXPECT_EQ("102", dataDescriptions[2]["full_processor_count"]);
    }
    {
        DataDescriptions dataDescriptions;
        string jsonStr = R"(
        [
            {
                "src" : "file",
                "type" : "0",
                "full_processor_count" : "100"
            },
            {
                "src" : "odps",
                "type" : "1",
                "full_processor_count" : "110s"
            },
            {
                "src" : "swift",
                "type" : "2",
                "full_processor_count" : "102"
            }
        ]
        )";
        autil::legacy::FromJsonString(dataDescriptions.toVector(), jsonStr);
        ASSERT_FALSE(dataDescriptions.validate());
    }
    {
        DataDescriptions dataDescriptions;
        string jsonStr = R"(
        [
            {
                "src" : "file",
                "type" : "0"
            },
            {
                "src" : "odps",
                "type" : "1",
                "full_processor_count" : "0"
            }
        ]
        )";
        autil::legacy::FromJsonString(dataDescriptions.toVector(), jsonStr);
        ASSERT_FALSE(dataDescriptions.validate());
    }
}

TEST_F(DataDescriptionsTest, testParserConfig)
{
    {
        DataDescriptions dataDescriptions;
        string jsonStr = R"(
        [
            {
                "src" : "file",
                "type" : "file", 
                "data" : "hdfs://xxx/mainse/xxx-file",
                "parser_config" : [
                    {
                        "type" : "isearch",
                        "parameters" : {
                            "kv_separator" : "="
                        }
                    }
                ]
            },
            {
                "src" : "swift",
                "type" : "swift",
                "swift_root" : "zfs://xxx-swift/xxx-swift-service",
                "parser_config" : [
                    {
                        "type" : "field_filter",
                        "parameters" : {}
                    },
                    {
                        "type" : "isearch",
                        "parameters" : {
                            "kv_separator" : "="
                        }
                    }        
                ]
            }
        ]
        )";
        //"""
        autil::legacy::FromJsonString(dataDescriptions.toVector(), jsonStr);
        ASSERT_TRUE(dataDescriptions.validate());
        ASSERT_EQ(2u, dataDescriptions.size());

        DataDescription ds = dataDescriptions[0];
        ASSERT_EQ(1u, ds.getParserConfigCount());
        ParserConfig parserConfig = ds.getParserConfig(0);
        EXPECT_EQ(string("isearch"), parserConfig.type);
        EXPECT_EQ(string("="), parserConfig.parameters["kv_separator"]);

        ds = dataDescriptions[1];
        ASSERT_EQ(2u, ds.getParserConfigCount());
        parserConfig = ds.getParserConfig(0);
        EXPECT_EQ(string("field_filter"), parserConfig.type);
        EXPECT_EQ(0u, parserConfig.parameters.size());
        parserConfig = ds.getParserConfig(1);
        EXPECT_EQ(string("isearch"), parserConfig.type);
        EXPECT_EQ(string("="), parserConfig.parameters["kv_separator"]);

        string toJsonStr = autil::legacy::ToJsonString(dataDescriptions.toVector());
        cout << toJsonStr << endl;
        DataDescriptions newDataDescriptions;
        autil::legacy::FromJsonString(newDataDescriptions.toVector(), toJsonStr);
        ASSERT_TRUE(newDataDescriptions.validate());
        ASSERT_EQ(2u, newDataDescriptions.size());
        ASSERT_EQ(dataDescriptions[0], newDataDescriptions[0]);
        ASSERT_EQ(dataDescriptions[1], newDataDescriptions[1]);
    }
}
}} // namespace build_service::proto
