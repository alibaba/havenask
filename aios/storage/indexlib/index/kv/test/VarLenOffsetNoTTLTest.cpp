#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"
#include "indexlib/index/kv/test/VarLenIndexerReadWriteTestBase.h"

namespace indexlibv2::index {

class VarLenOffsetNoTTLTest : public VarLenIndexerReadWriteTestBase
{
protected:
    void MakeSchema(const std::string& fieldNames, const std::string& keyName, const std::string& valueNames) override
    {
        KVIndexConfigBuilder builder(fieldNames, keyName, valueNames);
        std::vector<std::string> valueFormats = {"", "impact", "plain"};
        size_t idx = GET_CLASS_NAME().size() % 3;
        builder.SetValueFormat(valueFormats[idx]);

        ASSERT_TRUE(builder.Valid());
        auto result = builder.SetEnableShortenOffset(false).Finalize();
        _schema = std::move(result.first);
        _indexConfig = std::move(result.second);
        auto typeId = MakeKVTypeId(*_indexConfig, nullptr);
        ASSERT_TRUE(typeId.isVarLen);
        ASSERT_FALSE(typeId.shortOffset);
        ASSERT_FALSE(typeId.fileCompress);
        ASSERT_FALSE(typeId.hasTTL);
    }
};

TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadInt8) { DoTestBuildAndRead<ft_int8>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadInt8WithDumpSort) { DoTestBuildAndRead<ft_int8>(true); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadInt16) { DoTestBuildAndRead<ft_int16>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadInt32) { DoTestBuildAndRead<ft_int32>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadInt64) { DoTestBuildAndRead<ft_int64>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadUInt8) { DoTestBuildAndRead<ft_uint8>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadUInt16) { DoTestBuildAndRead<ft_uint16>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadUInt32) { DoTestBuildAndRead<ft_uint32>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadUInt64) { DoTestBuildAndRead<ft_uint64>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadFloat) { DoTestBuildAndRead<ft_float>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadFloatWithDumpSort) { DoTestBuildAndRead<ft_float>(true); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadDouble) { DoTestBuildAndRead<ft_double>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadString) { DoTestBuildAndRead<ft_string>(); }
TEST_F(VarLenOffsetNoTTLTest, testBuildAndReadStringWithDumpSort) { DoTestBuildAndRead<ft_string>(true); }
TEST_F(VarLenOffsetNoTTLTest, testCheckBuildMemoryFull) { CheckBuildMemoryFull(); }

} // namespace indexlibv2::index
