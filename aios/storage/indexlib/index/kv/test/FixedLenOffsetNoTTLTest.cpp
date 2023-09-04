#include "indexlib/index/kv/KVTypeId.h"
#include "indexlib/index/kv/test/FixedLenIndexerReadWriteTestBase.h"
#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"

namespace indexlibv2::index {

class FixedLenOffsetNoTTLTest : public FixedLenIndexerReadWriteTestBase
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
    }
};

TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadInt8) { DoTestBuildAndRead<ft_int8>(); }
TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadInt16) { DoTestBuildAndRead<ft_int16>(); }
TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadInt32) { DoTestBuildAndRead<ft_int32>(); }
TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadInt64) { DoTestBuildAndRead<ft_int64>(); }
TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadUInt8) { DoTestBuildAndRead<ft_uint8>(); }
TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadUInt16) { DoTestBuildAndRead<ft_uint16>(); }
TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadUInt32) { DoTestBuildAndRead<ft_uint32>(); }
TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadUInt64) { DoTestBuildAndRead<ft_uint64>(); }
TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadFloat) { DoTestBuildAndRead<ft_float>(); }
TEST_F(FixedLenOffsetNoTTLTest, testBuildAndReadDouble) { DoTestBuildAndRead<ft_double>(); }
TEST_F(FixedLenOffsetNoTTLTest, testCheckBuildMemoryFull) { CheckBuildMemoryFull(); }

} // namespace indexlibv2::index
