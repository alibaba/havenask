#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/test/FixedLenIndexerReadWriteTestBase.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"

namespace indexlibv2::index {

class FixedLenValueCompressTest : public FixedLenIndexerReadWriteTestBase
{
protected:
    void MakeSchema(const std::string& fieldNames, const std::string& keyName, const std::string& valueNames,
                    std::string compressTypeStr = "") override
    {
        KVIndexConfigBuilder builder(fieldNames, keyName, valueNames);
        ASSERT_TRUE(builder.Valid());
        auto result = builder.SetEnableShortenOffset(false).Finalize();
        _schema = std::move(result.first);
        _indexConfig = std::move(result.second);
        auto typeId = MakeKVTypeId(*_indexConfig, nullptr);
        ASSERT_FALSE(typeId.isVarLen);
        ASSERT_FALSE(typeId.shortOffset);
        ASSERT_FALSE(typeId.fileCompress);
        ASSERT_FALSE(typeId.hasTTL);
        if (compressTypeStr == "fp16") {
            ASSERT_EQ(2u, typeId.valueLen);
            ASSERT_EQ(2, typeId.compressTypeFlag);
        }
        if (compressTypeStr == "int8") {
            ASSERT_EQ(1u, typeId.valueLen);
            ASSERT_EQ(1, typeId.compressTypeFlag);
        }
    }
};

TEST_F(FixedLenValueCompressTest, testBuildAndReadFloatFp16) { DoTestBuildAndRead<ft_float>("fp16", ":false:fp16"); }
TEST_F(FixedLenValueCompressTest, testBuildAndReadFloatInt8)
{
    DoTestBuildAndRead<ft_float>("int8", ":false:int8#127");
}

} // namespace indexlibv2::index
