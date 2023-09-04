#include "indexlib/config/CompressTypeOption.h"

#include "unittest/unittest.h"

using namespace std;

namespace indexlib { namespace config {
class CompressTypeOptionTest : public TESTBASE
{
public:
    CompressTypeOptionTest();
    ~CompressTypeOptionTest();
};

CompressTypeOptionTest::CompressTypeOptionTest() {}

CompressTypeOptionTest::~CompressTypeOptionTest() {}

TEST_F(CompressTypeOptionTest, TestInit)
{
    {
        string compressType = "uniq | equal";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_TRUE(type.HasUniqEncodeCompress());
        ASSERT_TRUE(type.HasEquivalentCompress());
    }

    {
        string compressType = "uniq";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_TRUE(type.HasUniqEncodeCompress());
        ASSERT_FALSE(type.HasEquivalentCompress());
    }

    {
        string compressType = "equal";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_FALSE(type.HasUniqEncodeCompress());
        ASSERT_TRUE(type.HasEquivalentCompress());
    }

    {
        string compressType = "equal|";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_FALSE(type.HasUniqEncodeCompress());
        ASSERT_TRUE(type.HasEquivalentCompress());
    }

    {
        string compressType = "equal|notExist";
        CompressTypeOption type;
        ASSERT_FALSE(type.Init(compressType).IsOK());
    }

    {
        string compressType = "";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
    }

    {
        string compressType = "uniq|block_fp";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_TRUE(type.HasUniqEncodeCompress());
        ASSERT_TRUE(type.HasBlockFpEncodeCompress());
    }

    {
        string compressType = "fp16";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_TRUE(type.HasFp16EncodeCompress());
    }

    {
        string compressType = "block_fp|fp16";
        CompressTypeOption type;
        ASSERT_FALSE(type.Init(compressType).IsOK());
    }
}

TEST_F(CompressTypeOptionTest, TestGetCompressStr)
{
    {
        string compressType = "equal";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_EQ(type.GetCompressStr(), compressType);
    }

    {
        string compressType = "uniq|equal";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_EQ(type.GetCompressStr(), compressType);
    }

    {
        string compressType = "uniq|";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_EQ(type.GetCompressStr(), string("uniq"));
    }

    {
        CompressTypeOption type;
        ASSERT_TRUE(type.GetCompressStr().empty());
    }

    {
        string compressType = "uniq|fp16";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_EQ(type.GetCompressStr(), string("uniq|fp16"));
    }

    {
        string compressType = "block_fp";
        CompressTypeOption type;
        ASSERT_TRUE(type.Init(compressType).IsOK());
        ASSERT_EQ(type.GetCompressStr(), string("block_fp"));
    }
}

TEST_F(CompressTypeOptionTest, TestAssertEqual)
{
    {
        CompressTypeOption type;
        ASSERT_TRUE(type.Init("equal|uniq").IsOK());
        CompressTypeOption other;
        ASSERT_TRUE(other.Init("equal|uniq").IsOK());
        ASSERT_TRUE(type.AssertEqual(other).IsOK());
    }

    {
        CompressTypeOption type;
        ASSERT_TRUE(type.Init("equal|uniq").IsOK());
        CompressTypeOption other;
        ASSERT_TRUE(other.Init("equal").IsOK());
        ASSERT_FALSE(type.AssertEqual(other).IsOK());
    }

    {
        CompressTypeOption type;
        ASSERT_TRUE(type.Init("equal|uniq").IsOK());
        CompressTypeOption other;
        ASSERT_TRUE(other.Init("uniq").IsOK());
        ASSERT_FALSE(type.AssertEqual(other).IsOK());
    }

    {
        CompressTypeOption type;
        ASSERT_TRUE(type.Init("equal|fp16").IsOK());
        CompressTypeOption other;
        ASSERT_TRUE(other.Init("fp16|equal").IsOK());
        ASSERT_TRUE(type.AssertEqual(other).IsOK());
    }

    {
        CompressTypeOption type;
        ASSERT_TRUE(type.Init("block_fp|uniq").IsOK());
        CompressTypeOption other;
        ASSERT_TRUE(other.Init("block_fp").IsOK());
        ASSERT_FALSE(type.AssertEqual(other).IsOK());
    }
}
}} // namespace indexlib::config
