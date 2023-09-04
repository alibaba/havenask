#include "indexlib/util/buffer_compressor/ZlibDefaultCompressor.h"

#include "BufferCompressorTest.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {

class ZlibDefaultCompressorTest : public BufferCompressorTest
{
public:
    ZlibDefaultCompressorTest();
    ~ZlibDefaultCompressorTest();

    DECLARE_CLASS_NAME(ZlibDefaultCompressorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSetCompressLevel();

private:
    BufferCompressorPtr CreateCompressor() const override;
};

INDEXLIB_UNIT_TEST_CASE(ZlibDefaultCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(ZlibDefaultCompressorTest, TestCaseForCompressLargeData);
INDEXLIB_UNIT_TEST_CASE(ZlibDefaultCompressorTest, TestSetCompressLevel);

using namespace std;

ZlibDefaultCompressorTest::ZlibDefaultCompressorTest() {}

ZlibDefaultCompressorTest::~ZlibDefaultCompressorTest() {}

void ZlibDefaultCompressorTest::CaseSetUp() {}

void ZlibDefaultCompressorTest::CaseTearDown() {}

BufferCompressorPtr ZlibDefaultCompressorTest::CreateCompressor() const
{
    return BufferCompressorPtr(new ZlibDefaultCompressor);
}

void ZlibDefaultCompressorTest::TestSetCompressLevel()
{
    ZlibDefaultCompressor compressor;
    ASSERT_EQ(Z_DEFAULT_COMPRESSION, compressor._compressLevel);

    {
        util::KeyValueMap param = {{"compress_level", "2"}};
        ZlibDefaultCompressor compressor(param);
        ASSERT_EQ(2, compressor._compressLevel);
    }

    {
        util::KeyValueMap param = {{"compress_level", "-10"}};
        ZlibDefaultCompressor compressor(param);
        ASSERT_EQ(Z_DEFAULT_COMPRESSION, compressor._compressLevel);
    }

    {
        util::KeyValueMap param = {{"compress_level", "10"}};
        ZlibDefaultCompressor compressor(param);
        ASSERT_EQ(Z_DEFAULT_COMPRESSION, compressor._compressLevel);
    }
}
}} // namespace indexlib::util
