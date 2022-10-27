#include "indexlib/partition/operation_queue/test/compress_operation_writer_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CompressOperationWriterTest);

CompressOperationWriterTest::CompressOperationWriterTest()
{
}

CompressOperationWriterTest::~CompressOperationWriterTest()
{
}

void CompressOperationWriterTest::CaseSetUp()
{
}

void CompressOperationWriterTest::CaseTearDown()
{
}

void CompressOperationWriterTest::TestSimpleProcess()
{
    {
        size_t size = (size_t)20 * 1024 * 1024;
        CompressOperationWriter compressWriter(size);
        ASSERT_EQ((size_t)1 * 1024 * 1024, compressWriter.mMaxOpBlockSerializeSize);
    }

    {
        size_t size = (size_t)256 * 1024 * 1024;
        CompressOperationWriter compressWriter(size);
        ASSERT_EQ((size_t)4 * 1024 * 1024, compressWriter.mMaxOpBlockSerializeSize);
    }

    {
        size_t size = (size_t)8 * 1024 * 1024 * 1024;
        CompressOperationWriter compressWriter(size);
        ASSERT_EQ((size_t)64 * 1024 * 1024, compressWriter.mMaxOpBlockSerializeSize);
    }
}

IE_NAMESPACE_END(partition);

