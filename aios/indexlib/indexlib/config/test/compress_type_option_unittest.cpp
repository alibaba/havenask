#include "indexlib/config/test/compress_type_option_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, CompressTypeOptionTest);

CompressTypeOptionTest::CompressTypeOptionTest()
{
}

CompressTypeOptionTest::~CompressTypeOptionTest()
{
}

void CompressTypeOptionTest::SetUp()
{
}

void CompressTypeOptionTest::TearDown()
{
}

void CompressTypeOptionTest::TestInit()
{
    {
        string compressType = "uniq | equal";
        CompressTypeOption type;
        type.Init(compressType);
        ASSERT_TRUE(type.HasUniqEncodeCompress());
        ASSERT_TRUE(type.HasEquivalentCompress());
    }

    {
        string compressType = "uniq";
        CompressTypeOption type;
        type.Init(compressType);
        ASSERT_TRUE(type.HasUniqEncodeCompress());
        ASSERT_FALSE(type.HasEquivalentCompress());
    }

    {
        string compressType = "equal";
        CompressTypeOption type;
        type.Init(compressType);
        ASSERT_FALSE(type.HasUniqEncodeCompress());
        ASSERT_TRUE(type.HasEquivalentCompress());
    }

    {
        string compressType = "equal|";
        CompressTypeOption type;
        type.Init(compressType);
        ASSERT_FALSE(type.HasUniqEncodeCompress());
        ASSERT_TRUE(type.HasEquivalentCompress());
    }

    {
        string compressType = "equal|notExist";
        CompressTypeOption type;
        ASSERT_ANY_THROW(type.Init(compressType));
    }

    {
        string compressType = "";
        CompressTypeOption type;
        ASSERT_NO_THROW(type.Init(compressType));
    }

    {
        string compressType = "uniq|block_fp";
        CompressTypeOption type;
        ASSERT_NO_THROW(type.Init(compressType));
        ASSERT_TRUE(type.HasUniqEncodeCompress());
        ASSERT_TRUE(type.HasBlockFpEncodeCompress());
    }
    
    {
        string compressType = "fp16";
        CompressTypeOption type;
        ASSERT_NO_THROW(type.Init(compressType));
        ASSERT_TRUE(type.HasFp16EncodeCompress());
    }
    
    {
        string compressType = "block_fp|fp16";
        CompressTypeOption type;
        ASSERT_ANY_THROW(type.Init(compressType));
    }

}

void CompressTypeOptionTest::TestGetCompressStr()
{
    {
        string compressType = "equal";
        CompressTypeOption type;
        type.Init(compressType);
        ASSERT_EQ(type.GetCompressStr(), compressType);
    }

    {
        string compressType = "uniq|equal";
        CompressTypeOption type;
        type.Init(compressType);
        ASSERT_EQ(type.GetCompressStr(), compressType);
    }

    {
        string compressType = "uniq|";
        CompressTypeOption type;
        type.Init(compressType);
        ASSERT_EQ(type.GetCompressStr(), string("uniq"));
    }

    {
        CompressTypeOption type;
        ASSERT_TRUE(type.GetCompressStr().empty());
    }

    {
        string compressType = "uniq|fp16";
        CompressTypeOption type;
        type.Init(compressType);
        ASSERT_EQ(type.GetCompressStr(), string("uniq|fp16"));
    }

    {
        string compressType = "block_fp";
        CompressTypeOption type;
        type.Init(compressType);
        ASSERT_EQ(type.GetCompressStr(), string("block_fp"));
    }

}

void CompressTypeOptionTest::TestAssertEqual()
{
    {
        CompressTypeOption type;
        type.Init("equal|uniq");
        CompressTypeOption other;
        other.Init("equal|uniq");
        ASSERT_NO_THROW(type.AssertEqual(other));
    }

    {
        CompressTypeOption type;
        type.Init("equal|uniq");
        CompressTypeOption other;
        other.Init("equal");
        ASSERT_ANY_THROW(type.AssertEqual(other));
    }

    {
        CompressTypeOption type;
        type.Init("equal|uniq");
        CompressTypeOption other;
        other.Init("uniq");
        ASSERT_ANY_THROW(type.AssertEqual(other));
    }

    {
        CompressTypeOption type;
        type.Init("equal|fp16");
        CompressTypeOption other;
        other.Init("fp16|equal");
        ASSERT_NO_THROW(type.AssertEqual(other));
    }

    {
        CompressTypeOption type;
        type.Init("block_fp|uniq");
        CompressTypeOption other;
        other.Init("block_fp");
        ASSERT_ANY_THROW(type.AssertEqual(other));
    }
}

IE_NAMESPACE_END(config);

