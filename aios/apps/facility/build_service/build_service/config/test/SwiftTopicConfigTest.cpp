#include "build_service/config/SwiftTopicConfig.h"

#include <cstdint>
#include <iosfwd>
#include <map>
#include <string>

#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/test/unittest.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace swift::protocol;
using namespace autil::legacy;

namespace build_service { namespace config {

class SwiftTopicConfigTest : public BUILD_SERVICE_TESTBASE
{
};

TEST_F(SwiftTopicConfigTest, testSimple)
{
    {
        SwiftTopicConfig config;
        string jsonStr = ToJsonString(config);
        FromJsonString(config, jsonStr);
        EXPECT_EQ(TOPIC_MODE_NORMAL, config.get().topicmode());
        EXPECT_EQ(1, config.get().partitioncount());
        EXPECT_EQ(1, config.get().resource());
        EXPECT_EQ(false, config.get().compressmsg());
        EXPECT_EQ(false, config.get().deletetopicdata());
        EXPECT_EQ("", config.readerConfigStr);
        EXPECT_EQ("", config.writerConfigStr);
    }
    {
        SwiftTopicConfig config;
        string configStr = R"raw({
        "partition_count" : 5,
        "topicMode" : "TOPIC_MODE_SECURITY",
        "partition_resource" : 2,
        "resource" : 3})raw";
        FromJsonString(config, configStr);
        EXPECT_EQ(TOPIC_MODE_SECURITY, config.get().topicmode());
        EXPECT_EQ(5, config.get().partitioncount());
        EXPECT_EQ(2, config.get().resource());
        EXPECT_EQ("default", config.get().topicgroup());
    }
    {
        SwiftTopicConfig config;
        string configStr = R"raw({
        "topic_mode" : "normal",
        "topicMode" : "TOPIC_MODE_SECURITY",
        "topicGroup" : "g1"})raw";
        FromJsonString(config, configStr);
        EXPECT_EQ(TOPIC_MODE_NORMAL, config.get().topicmode());
        EXPECT_EQ("g1", config.get().topicgroup());
    }
    {
        SwiftTopicConfig config;
        string configStr = R"raw({
        "topicGroup" : "g1",
        "topic_group" : "g2",
        "topicMode" : "TOPIC_MODE_SECURITY"})raw";
        FromJsonString(config, configStr);
        EXPECT_EQ(TOPIC_MODE_SECURITY, config.get().topicmode());
        EXPECT_EQ("g2", config.get().topicgroup());
    }
    {
        SwiftTopicConfig config;
        string configStr = R"raw({
        "topicGroup" : "g1",
        "topic_group" : "g2",
        "topic_mode" : "memory_prefer",
        "builder_mem_to_processor_buffer_factor" : 1.9,
        "" : ""})raw";
        FromJsonString(config, configStr);
        EXPECT_EQ(TOPIC_MODE_MEMORY_PREFER, config.get().topicmode());
        EXPECT_EQ("g2", config.get().topicgroup());
        EXPECT_EQ(1.9, config.builderMemToProcessorBufferFactor);
    }
    {
        SwiftTopicConfig config;
        string configStr = R"({
            "topic_mode" : "memory_prefer",
            "writer_max_buffer_size" : {
                 "simple.file" : 10,
                 "simple.swift" : 100
            }
        })";
        FromJsonString(config, configStr);
        EXPECT_EQ(TOPIC_MODE_MEMORY_PREFER, config.get().topicmode());
        EXPECT_EQ(10u, config.writerMaxBufferSize["simple.file"]);
        EXPECT_EQ(100u, config.writerMaxBufferSize["simple.swift"]);
    }
}

TEST_F(SwiftTopicConfigTest, testValidate)
{
    {
        SwiftTopicConfig config;
        string configStr = R"raw({
        "partition_count" : 5,
        "topic_mode" : "memory_only",
        "partition_resource" : 2,
        "resource" : 3})raw";
        FromJsonString(config, configStr);
        EXPECT_FALSE(config.validate());
    }
    {
        SwiftTopicConfig config;
        string configStr = R"raw({
        "partition_count" : 5,
        "topic_mode" : "memory_prefer",
        "partition_resource" : 2,
        "resource" : 3,
        "no_more_msg_period": -4})raw";
        FromJsonString(config, configStr);
        EXPECT_FALSE(config.validate(true));
    }
    {
        SwiftTopicConfig config;
        string configStr = R"raw({
        "partition_count" : 5,
        "topic_mode" : "memory_prefer",
        "partition_resource" : 2,
        "resource" : 3})raw";
        FromJsonString(config, configStr);
        EXPECT_TRUE(config.validate(true));
        EXPECT_EQ(SwiftTopicConfig::DEFAULT_NO_MORE_MSG_PERIOD, config.noMoreMsgPeriod);
        EXPECT_EQ(SwiftTopicConfig::DEFAULT_BULDER_MEM_TO_PROCESSOR_BUFFER_FACTOR,
                  config.builderMemToProcessorBufferFactor);
    }
}

}} // namespace build_service::config
