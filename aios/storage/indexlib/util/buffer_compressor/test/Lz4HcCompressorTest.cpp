#include "indexlib/util/buffer_compressor/Lz4HcCompressor.h"

#include "BufferCompressorTest.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {

class Lz4HcCompressorTest : public BufferCompressorTest
{
public:
    Lz4HcCompressorTest();
    ~Lz4HcCompressorTest();

    DECLARE_CLASS_NAME(Lz4HcCompressorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSetCompressLevel();

private:
    util::BufferCompressorPtr CreateCompressor() const override;
};

INDEXLIB_UNIT_TEST_CASE(Lz4HcCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(Lz4HcCompressorTest, TestCaseForCompressLargeData);
INDEXLIB_UNIT_TEST_CASE(Lz4HcCompressorTest, TestSetCompressLevel);

using namespace std;

Lz4HcCompressorTest::Lz4HcCompressorTest() : BufferCompressorTest(false) {}

Lz4HcCompressorTest::~Lz4HcCompressorTest() {}

void Lz4HcCompressorTest::CaseSetUp() {}

void Lz4HcCompressorTest::CaseTearDown() {}

BufferCompressorPtr Lz4HcCompressorTest::CreateCompressor() const { return BufferCompressorPtr(new Lz4HcCompressor); }

void Lz4HcCompressorTest::TestSetCompressLevel()
{
    Lz4HcCompressor compressor;
    ASSERT_EQ(LZ4HC_CLEVEL_DEFAULT, compressor._compressLevel);

    {
        util::KeyValueMap param = {{"compress_level", "8"}};
        Lz4HcCompressor compressor(param);
        ASSERT_EQ(8, compressor._compressLevel);
    }

    {
        util::KeyValueMap param = {{"compress_level", "17"}};
        Lz4HcCompressor compressor(param);
        ASSERT_EQ(LZ4HC_CLEVEL_DEFAULT, compressor._compressLevel);
    }
}

}} // namespace indexlib::util
