#include "indexlib/util/buffer_compressor/test/zlib_default_compressor_unittest.h"
#include "indexlib/util/buffer_compressor/zlib_default_compressor.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ZlibDefaultCompressorTest);

ZlibDefaultCompressorTest::ZlibDefaultCompressorTest()
{
}

ZlibDefaultCompressorTest::~ZlibDefaultCompressorTest()
{
}

void ZlibDefaultCompressorTest::CaseSetUp()
{
}

void ZlibDefaultCompressorTest::CaseTearDown()
{
}

BufferCompressorPtr ZlibDefaultCompressorTest::CreateCompressor() const
{
    return BufferCompressorPtr(new ZlibDefaultCompressor);
}

IE_NAMESPACE_END(util);

