#include "indexlib/util/buffer_compressor/test/snappy_compressor_unittest.h"
#include "indexlib/util/buffer_compressor/snappy_compressor.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, SnappyCompressorTest);

SnappyCompressorTest::SnappyCompressorTest()
{
}

SnappyCompressorTest::~SnappyCompressorTest()
{
}

void SnappyCompressorTest::CaseSetUp()
{
}

void SnappyCompressorTest::CaseTearDown()
{
}

BufferCompressorPtr SnappyCompressorTest::CreateCompressor() const
{
    return BufferCompressorPtr(new SnappyCompressor);
}

IE_NAMESPACE_END(util);

