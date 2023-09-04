#include "suez/drc/DrcConfig.h"

#include "unittest/unittest.h"

namespace suez {

class DrcConfigTest : public TESTBASE {};

TEST_F(DrcConfigTest, testConstructor) {
    DrcConfig config;
    ASSERT_FALSE(config.isEnabled());
    ASSERT_TRUE(config.shareMode());
}

TEST_F(DrcConfigTest, testJsonize) {
    std::string jsonStr = R"doc({
"enabled" : true,
"checkpoint_root" : "zfs://xx",
"start_offset" : 1024,
"share_mode" : false,
"source" : {
    "type" : "swift",
    "parameters" : {}
},
"sinks" :[
    {"type" : "swift", "name" : "biz_order_buyer"}
]
})doc";

    DrcConfig config;
    try {
        FastFromJsonString(config, jsonStr);
    } catch (...) { ASSERT_TRUE(false) << "parse from " << jsonStr << " failed"; }
    ASSERT_TRUE(config.isEnabled());
    ASSERT_FALSE(config.shareMode());
    ASSERT_EQ(1024, config.getStartOffset());
    ASSERT_EQ("zfs://xx", config.getCheckpointRoot());
    ASSERT_EQ("swift", config.getSourceConfig().type);
    ASSERT_EQ(1, config.getSinkConfigs().size());
    ASSERT_EQ("biz_order_buyer", config.getSinkConfigs()[0].name);
    ASSERT_EQ("swift", config.getSinkConfigs()[0].type);
}

TEST_F(DrcConfigTest, testDeriveForSingleSink) {
    DrcConfig config;
    config._startOffset = 100;
    config._checkpointRoot = "zfs://root1";
    SinkConfig sinkConfig1;
    sinkConfig1.name = "buyer";
    sinkConfig1.type = "swift";
    config._sinkConfigs.push_back(sinkConfig1);

    SinkConfig sinkConfig2;
    sinkConfig2.name = "seller";
    sinkConfig2.type = "swift";
    sinkConfig2.sourceStartOffset = 102;
    config._sinkConfigs.push_back(sinkConfig2);

    auto config1 = config.deriveForSingleSink(sinkConfig1);
    ASSERT_EQ("zfs://root1/buyer", config1._checkpointRoot);
    ASSERT_EQ(100, config1._startOffset);
    ASSERT_EQ(1, config1._sinkConfigs.size());

    auto config2 = config.deriveForSingleSink(sinkConfig2);
    ASSERT_EQ("zfs://root1/seller", config2._checkpointRoot);
    ASSERT_EQ(102, config2._startOffset);
    ASSERT_EQ(1, config2._sinkConfigs.size());
}

} // namespace suez
