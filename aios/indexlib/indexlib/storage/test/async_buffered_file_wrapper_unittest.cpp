#include "indexlib/storage/test/async_buffered_file_wrapper_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, AsyncBufferedFileWrapperTest);

INDEXLIB_UNIT_TEST_CASE(AsyncBufferedFileWrapperTest, TestCaseForRead);
INDEXLIB_UNIT_TEST_CASE(AsyncBufferedFileWrapperTest, TestCaseForSeek);
INDEXLIB_UNIT_TEST_CASE(AsyncBufferedFileWrapperTest, TestCaseForWrite);
INDEXLIB_UNIT_TEST_CASE(AsyncBufferedFileWrapperTest, TestCaseForWriteMultiTimes);

void AsyncBufferedFileWrapperTest::CaseSetUp()
{
    BufferedFileWrapperTest::CaseSetUp();
}

void AsyncBufferedFileWrapperTest::CaseTearDown()
{
    BufferedFileWrapperTest::CaseTearDown();
}

IE_NAMESPACE_END(storage);

