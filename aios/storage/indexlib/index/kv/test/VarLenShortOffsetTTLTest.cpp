#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "indexlib/index/kv/test/VarLenIndexerReadWriteTestBase.h"

namespace indexlibv2::index {

class VarLenShortOffsetTTLTest : public VarLenIndexerReadWriteTestBase
{
protected:
    void MakeSchema(const std::string& fieldNames, const std::string& keyName, const std::string& valueNames) override
    {
        KVIndexConfigBuilder builder(fieldNames, keyName, valueNames);
        std::vector<std::string> valueFormats = {"", "impact", "plain"};
        size_t idx = GET_CLASS_NAME().size() % 3;
        builder.SetValueFormat(valueFormats[idx]);

        ASSERT_TRUE(builder.Valid());
        auto result = builder.SetTTL(1024).Finalize();
        _schema = std::move(result.first);
        _indexConfig = std::move(result.second);
        auto typeId = MakeKVTypeId(*_indexConfig, nullptr);
        ASSERT_TRUE(typeId.isVarLen);
        ASSERT_TRUE(typeId.shortOffset);
        ASSERT_FALSE(typeId.fileCompress);
        ASSERT_TRUE(typeId.hasTTL);
    }
};

TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadInt8) { DoTestBuildAndRead<ft_int8>(); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadInt16) { DoTestBuildAndRead<ft_int16>(); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadInt32) { DoTestBuildAndRead<ft_int32>(); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadInt64) { DoTestBuildAndRead<ft_int64>(); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadUInt8) { DoTestBuildAndRead<ft_uint8>(); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadUInt16) { DoTestBuildAndRead<ft_uint16>(true); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadUInt32) { DoTestBuildAndRead<ft_uint32>(); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadUInt64) { DoTestBuildAndRead<ft_uint64>(); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadFloat) { DoTestBuildAndRead<ft_float>(true); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadDouble) { DoTestBuildAndRead<ft_double>(); }
TEST_F(VarLenShortOffsetTTLTest, testBuildAndReadString) { DoTestBuildAndRead<ft_string>(); }

} // namespace indexlibv2::index
