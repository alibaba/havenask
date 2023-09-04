#include "indexlib/util/buffer_compressor/SnappyCompressor.h"

#include "BufferCompressorTest.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {

class SnappyCompressorTest : public BufferCompressorTest
{
public:
    SnappyCompressorTest();
    ~SnappyCompressorTest();

    DECLARE_CLASS_NAME(SnappyCompressorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    util::BufferCompressorPtr CreateCompressor() const override;
};

INDEXLIB_UNIT_TEST_CASE(SnappyCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(SnappyCompressorTest, TestCaseForCompressLargeData);

using namespace std;

SnappyCompressorTest::SnappyCompressorTest() {}

SnappyCompressorTest::~SnappyCompressorTest() {}

void SnappyCompressorTest::CaseSetUp() {}

void SnappyCompressorTest::CaseTearDown() {}

BufferCompressorPtr SnappyCompressorTest::CreateCompressor() const { return BufferCompressorPtr(new SnappyCompressor); }
}} // namespace indexlib::util
