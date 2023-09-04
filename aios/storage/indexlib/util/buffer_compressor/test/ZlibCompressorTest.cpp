#include "indexlib/util/buffer_compressor/ZlibCompressor.h"

#include "BufferCompressorTest.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {

class ZlibCompressorTest : public BufferCompressorTest
{
public:
    ZlibCompressorTest();
    ~ZlibCompressorTest();

    DECLARE_CLASS_NAME(ZlibCompressorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSetCompressLevel();

private:
    BufferCompressorPtr CreateCompressor() const override;
};

INDEXLIB_UNIT_TEST_CASE(ZlibCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(ZlibCompressorTest, TestCaseForCompressLargeData);
INDEXLIB_UNIT_TEST_CASE(ZlibCompressorTest, TestSetCompressLevel);

using namespace std;

ZlibCompressorTest::ZlibCompressorTest() {}

ZlibCompressorTest::~ZlibCompressorTest() {}

void ZlibCompressorTest::CaseSetUp() {}

void ZlibCompressorTest::CaseTearDown() {}

BufferCompressorPtr ZlibCompressorTest::CreateCompressor() const { return BufferCompressorPtr(new ZlibCompressor); }

void ZlibCompressorTest::TestSetCompressLevel()
{
    ZlibCompressor compressor;
    ASSERT_EQ(Z_BEST_SPEED, compressor._compressLevel);

    {
        util::KeyValueMap param = {{"compress_level", "2"}};
        ZlibCompressor compressor(param);
        ASSERT_EQ(2, compressor._compressLevel);
    }

    {
        util::KeyValueMap param = {{"compress_level", "-10"}};
        ZlibCompressor compressor(param);
        ASSERT_EQ(Z_BEST_SPEED, compressor._compressLevel);
    }

    {
        util::KeyValueMap param = {{"compress_level", "10"}};
        ZlibCompressor compressor(param);
        ASSERT_EQ(Z_BEST_SPEED, compressor._compressLevel);
    }
}
}} // namespace indexlib::util
