#include "indexlib/storage/test/raid_config_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, RaidConfigTest);

RaidConfigTest::RaidConfigTest()
{
}

RaidConfigTest::~RaidConfigTest()
{
}

void RaidConfigTest::CaseSetUp()
{
}

void RaidConfigTest::CaseTearDown()
{
}

void RaidConfigTest::TestSimpleProcess() {
    {
        RaidConfig config;
        auto configStr = config.FsConfig("pangu");
        EXPECT_EQ(string(""), configStr);
    }
    {
        RaidConfig config;
        config.useRaid = true;
        auto configStr = config.FsConfig("pangu");
        EXPECT_EQ(string("use_raid=true"), configStr);
    }
    {
        RaidConfig config;
        config.useRaid = true;
        auto configStr = config.FsConfig("hdfs");
        EXPECT_EQ(string(""), configStr);
    }    
    {
        RaidConfig config;
        config.useRaid = true;
        config.dataBlockNum = 8;
        config.parityBlockNum = 3;
        config.packetSizeBits = 15;
        auto configStr = config.FsConfig("pangu");
        EXPECT_EQ(string("use_raid=true&raid_data_num=8&raid_parity_num=3&raid_packet_bits=15"), configStr);
    }
    {
        RaidConfig config;
        config.useRaid = true;
        config.dataBlockNum = 8;
        config.parityBlockNum = 3;
        config.packetSizeBits = 15;
        auto path = config.MakeRaidPath("pangu://ea120/a/b/c.data__0__");
        EXPECT_EQ(string("pangu://ea120?use_raid=true&raid_data_num=8&raid_parity_num=3&raid_packet_bits=15/a/b/c.data__0__"), path);
    }
    {
        RaidConfig config;
        config.useRaid = true;
        config.dataBlockNum = 8;
        config.parityBlockNum = 3;
        config.packetSizeBits = 15;
        auto path = config.MakeRaidPath("pangu://ea120?app=abc^as3&a=1/a/b/c.data__0__");
        EXPECT_EQ(string("pangu://ea120?app=abc^as3&a=1&use_raid=true&raid_data_num=8&raid_parity_num=3&raid_packet_bits=15/a/b/c.data__0__"), path);
    }
    {
        RaidConfig config;
        config.useRaid = true;
        config.dataBlockNum = 8;
        config.parityBlockNum = 3;
        config.packetSizeBits = 15;
        auto path = config.MakeRaidPath("hdfs://ea120?app=abc^as3&a=1/a/b/c.data__0__");
        EXPECT_EQ(string("hdfs://ea120?app=abc^as3&a=1/a/b/c.data__0__"), path);
    }        
}

IE_NAMESPACE_END(storage);

