#include "indexlib/util/buffer_compressor/test/zlib_compressor_unittest.h"
#include "indexlib/util/buffer_compressor/zlib_compressor.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ZlibCompressorTest);

ZlibCompressorTest::ZlibCompressorTest()
{
}

ZlibCompressorTest::~ZlibCompressorTest()
{
}

void ZlibCompressorTest::CaseSetUp()
{
}

void ZlibCompressorTest::CaseTearDown()
{
}

BufferCompressorPtr ZlibCompressorTest::CreateCompressor() const
{
    return BufferCompressorPtr(new ZlibCompressor);
}

IE_NAMESPACE_END(util);

