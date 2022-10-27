#include "indexlib/index/normal/inverted_index/perf_test/doc_list_encoder_perf_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocListEncoderPerfTest);

DocListEncoderPerfTest::DocListEncoderPerfTest()
    : mByteSlicePool(1024)
    , mBufferPool(1024)
{
}

DocListEncoderPerfTest::~DocListEncoderPerfTest()
{
}

void DocListEncoderPerfTest::SetUp()
{
}

void DocListEncoderPerfTest::TearDown()
{
}

void DocListEncoderPerfTest::TestDocListAddDocIdPerf()
{
    DocListFormatOption option(OPTION_FLAG_NONE);
    DocListEncoder docListEncoder(option, &mSimplePool, &mByteSlicePool, &mBufferPool);
    for (size_t i = 0; i < 50000000; ++i)
    {
        docListEncoder.EndDocument(i, i);
    }
}

void DocListEncoderPerfTest::TestDocListAddAllPerf()
{
    DocListFormatOption option(OPTION_FLAG_ALL);
    DocListEncoder docListEncoder(option, &mSimplePool, &mByteSlicePool, &mBufferPool);
    for (size_t i = 0; i < 50000000; ++i)
    {
        for (size_t j = 0; j < i % 3; ++j)
        {
            docListEncoder.AddPosition(j);
        }
        docListEncoder.EndDocument(i, i);
    }
}

IE_NAMESPACE_END(index);

