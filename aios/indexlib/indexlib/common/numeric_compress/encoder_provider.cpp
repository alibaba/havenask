#include "indexlib/common/numeric_compress/encoder_provider.h"
#include "indexlib/common/numeric_compress/new_pfordelta_int_encoder.h"
#include "indexlib/common/numeric_compress/no_compress_int_encoder.h"
#include "indexlib/common/numeric_compress/vbyte_int32_encoder.h"
#include "indexlib/common/numeric_compress/group_vint32_encoder.h"
#include "indexlib/common/numeric_compress/reference_compress_int_encoder.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, EncoderProvider);

EncoderProvider::EncoderProvider()
    : mDisableSseOptimize(false)
{
    Init();
}

EncoderProvider::~EncoderProvider() 
{
}

void EncoderProvider::Init()
{
    mInt32PForDeltaEncoder.reset(new NewPForDeltaInt32Encoder());
    mNoSseInt32PForDeltaEncoder.reset(new NoSseNewPForDeltaInt32Encoder());

    mInt16PForDeltaEncoder.reset(new NewPForDeltaInt16Encoder());
    mInt8PForDeltaEncoder.reset(new NewPForDeltaInt8Encoder());

    mInt32NoCompressEncoder.reset(new NoCompressInt32Encoder());
    mInt16NoCompressEncoder.reset(new NoCompressInt16Encoder());
    mInt8NoCompressEncoder.reset(new NoCompressInt8Encoder());

    mInt32NoCompressNoLengthEncoder.reset(new NoCompressInt32Encoder(false));
    mInt32VByteEncoder.reset(new VbyteInt32Encoder());
    mInt32ReferenceCompressEncoder.reset(new ReferenceCompressInt32Encoder());
}

IE_NAMESPACE_END(common);

