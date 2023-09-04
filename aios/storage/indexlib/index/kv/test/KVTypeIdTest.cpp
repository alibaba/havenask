#include "indexlib/index/kv/KVTypeId.h"

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "unittest/unittest.h"

namespace indexlibv2::index {

class KVTypeIdTest : public TESTBASE
{
};

TEST_F(KVTypeIdTest, testParseKVIndexType)
{
    EXPECT_EQ(KVIndexType::KIT_DENSE_HASH, ParseKVIndexType("dense"));
    EXPECT_EQ(KVIndexType::KIT_DENSE_HASH, ParseKVIndexType("DENSE"));
    EXPECT_EQ(KVIndexType::KIT_CUCKOO_HASH, ParseKVIndexType("cuckoo"));
    EXPECT_EQ(KVIndexType::KIT_DENSE_HASH, ParseKVIndexType("CUCKOO"));
    EXPECT_EQ(KVIndexType::KIT_DENSE_HASH, ParseKVIndexType("unknown"));
}

TEST_F(KVTypeIdTest, testPrintKVIndexType)
{
    EXPECT_EQ(std::string("dense"), std::string(PrintKVIndexType(KVIndexType::KIT_DENSE_HASH)));
    EXPECT_EQ(std::string("cuckoo"), std::string(PrintKVIndexType(KVIndexType::KIT_CUCKOO_HASH)));
    EXPECT_EQ(std::string("unknown"), std::string(PrintKVIndexType(KVIndexType::KIT_UNKOWN)));
}

TEST_F(KVTypeIdTest, testMakeTypeId)
{
    {
        // single value
        auto [_, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("f1:string;f2:long", "f1", "f2");
        ASSERT_TRUE(indexConfig);
        auto typeId = MakeKVTypeId(*indexConfig, nullptr);
        auto indexType = KVIndexType::KIT_DENSE_HASH;
        KVTypeId expected(indexType, indexType, ft_long, false, false, false, false, false, false, 8, ct_other,
                          indexlib::config::CompressTypeOption());
        EXPECT_EQ(expected, typeId) << typeId.ToString();
    }
    {
        // multiple value
        auto [_, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("f1:string;f2:long", "f1", "f1;f2");
        ASSERT_TRUE(indexConfig);
        auto typeId = MakeKVTypeId(*indexConfig, nullptr);
        auto indexType = KVIndexType::KIT_DENSE_HASH;
        KVTypeId expected(indexType, indexType, ft_unknown, true, false, false, false, true, false, 0, ct_other,
                          indexlib::config::CompressTypeOption());
        EXPECT_EQ(expected, typeId) << typeId.ToString();
    }
    {
        // simple value
        auto [_, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("f1:string;f2:int8", "f1", "f2");
        ASSERT_TRUE(indexConfig);
        auto typeId = MakeKVTypeId(*indexConfig, nullptr);
        auto indexType = KVIndexType::KIT_DENSE_HASH;
        KVTypeId expected(indexType, indexType, ft_int8, false, false, false, false, false, false, 1, ct_other,
                          indexlib::config::CompressTypeOption());
        EXPECT_EQ(expected, typeId) << typeId.ToString();
    }
    {
        // simple value : value len
        auto [_, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("f1:string;f2:int8", "f1", "f2");
        ASSERT_TRUE(indexConfig);
        auto typeId = MakeKVTypeId(*indexConfig, nullptr);
        auto indexType = KVIndexType::KIT_DENSE_HASH;
        KVTypeId expected(indexType, indexType, ft_int8, false, false, false, false, false, false, 1, ct_other,
                          indexlib::config::CompressTypeOption());
        EXPECT_EQ(expected, typeId) << typeId.ToString();
    }
    {
        // simple value : fp16 compress flag and type
        auto [_, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("f1:string;f2:float", "f1", "f2");
        ASSERT_TRUE(indexConfig);
        const auto& valueConfig = indexConfig->GetValueConfig();
        const auto& attrConfig = valueConfig->GetAttributeConfig(0);
        ASSERT_TRUE(attrConfig->SetCompressType("fp16").IsOK());
        auto typeId = MakeKVTypeId(*indexConfig, nullptr);
        auto indexType = KVIndexType::KIT_DENSE_HASH;
        indexlib::config::CompressTypeOption compressOption;
        ASSERT_TRUE(compressOption.Init("fp16").IsOK());
        KVTypeId expected(indexType, indexType, ft_float, false, false, false, false, false, false, 2, ct_fp16,
                          compressOption);
        EXPECT_EQ(expected, typeId) << typeId.ToString();
    }
    {
        // simple value : int8 compress flag and type
        auto [_, indexConfig] = KVIndexConfigBuilder::MakeIndexConfig("f1:string;f2:float", "f1", "f2");
        ASSERT_TRUE(indexConfig);
        const auto& valueConfig = indexConfig->GetValueConfig();
        const auto& attrConfig = valueConfig->GetAttributeConfig(0);
        ASSERT_TRUE(attrConfig->SetCompressType("int8#127").IsOK());
        auto typeId = MakeKVTypeId(*indexConfig, nullptr);
        auto indexType = KVIndexType::KIT_DENSE_HASH;
        indexlib::config::CompressTypeOption compressOption;
        ASSERT_TRUE(compressOption.Init("int8#127").IsOK());
        KVTypeId expected(indexType, indexType, ft_float, false, false, false, false, false, false, 1, ct_int8,
                          compressOption);
        EXPECT_EQ(expected, typeId) << typeId.ToString();
    }
}

} // namespace indexlibv2::index
