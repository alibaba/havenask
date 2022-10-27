#include "indexlib/util/buffer_compressor/test/lz4_hc_compressor_unittest.h"
#include "indexlib/util/buffer_compressor/lz4_hc_compressor.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, Lz4HcCompressorTest);

Lz4HcCompressorTest::Lz4HcCompressorTest()
    : BufferCompressorTest(false)
{
}

Lz4HcCompressorTest::~Lz4HcCompressorTest()
{
}

void Lz4HcCompressorTest::CaseSetUp()
{
}

void Lz4HcCompressorTest::CaseTearDown()
{
}

BufferCompressorPtr Lz4HcCompressorTest::CreateCompressor() const
{
    return BufferCompressorPtr(new Lz4HcCompressor);
}

IE_NAMESPACE_END(util);

