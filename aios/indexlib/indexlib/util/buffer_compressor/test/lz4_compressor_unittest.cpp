#include "indexlib/util/buffer_compressor/test/lz4_compressor_unittest.h"
#include "indexlib/util/buffer_compressor/lz4_compressor.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, Lz4CompressorTest);

Lz4CompressorTest::Lz4CompressorTest()
    : BufferCompressorTest(false)
{
}

Lz4CompressorTest::~Lz4CompressorTest()
{
}

void Lz4CompressorTest::CaseSetUp()
{
}

void Lz4CompressorTest::CaseTearDown()
{
}

BufferCompressorPtr Lz4CompressorTest::CreateCompressor() const
{
    return BufferCompressorPtr(new Lz4Compressor);
}

IE_NAMESPACE_END(util);

