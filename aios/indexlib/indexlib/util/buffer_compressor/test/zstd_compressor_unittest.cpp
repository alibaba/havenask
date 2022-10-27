#include "indexlib/util/buffer_compressor/test/zstd_compressor_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ZstdCompressorTest);

ZstdCompressorTest::ZstdCompressorTest()
{
}

ZstdCompressorTest::~ZstdCompressorTest()
{
}

void ZstdCompressorTest::CaseSetUp()
{
}

void ZstdCompressorTest::CaseTearDown()
{
}

BufferCompressorPtr ZstdCompressorTest::CreateCompressor() const
{
    return BufferCompressorPtr(new ZstdCompressor);
}

IE_NAMESPACE_END(util);

