#include "indexlib/index/normal/inverted_index/perf_test/position_list_encoder_perf_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PositionListEncoderPerfTest);

PositionListEncoderPerfTest::PositionListEncoderPerfTest()
    : mByteSlicePool(1024)
    , mBufferPool(1024)
{
}

PositionListEncoderPerfTest::~PositionListEncoderPerfTest()
{
}

void PositionListEncoderPerfTest::SetUp()
{
}

void PositionListEncoderPerfTest::TearDown()
{
}

void PositionListEncoderPerfTest::TestAddPositionWithoutPayload()
{
    PositionListFormatOption option(NO_PAYLOAD);
    PositionListEncoder positionListEncoder(option, &mByteSlicePool, &mBufferPool);
    for (size_t i = 0; i < 50000000; ++i)
    {
        positionListEncoder.AddPosition(i, i);
        positionListEncoder.EndDocument();
    }
}

void PositionListEncoderPerfTest::TestAddAllPerf()
{
    PositionListFormatOption option(OPTION_FLAG_ALL);
    PositionListEncoder positionListEncoder(option, &mByteSlicePool, &mBufferPool);
    for (size_t i = 0; i < 50000000; ++i)
    {
        positionListEncoder.AddPosition(i, i);
        positionListEncoder.EndDocument();
    }
}

IE_NAMESPACE_END(index);

