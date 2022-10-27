#include "indexlib/index/normal/inverted_index/format/skiplist/test/in_mem_tri_value_skip_list_reader_unittest.h"

using namespace std;


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemTriValueSkipListReaderTest);

INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForSkipToNotEqual);
INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForSkipToEqual);
INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForGetSkippedItemCount);
INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForGetLastValueInBuffer);
INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForGetLastValueInBufferForSeekFailed);
INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForSkipToNotEqualWithFlush);
INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForSkipToEqualWithFlush);
INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForGetSkippedItemCountWithFlush);
INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForGetLastValueInBufferForFlush);
INDEXLIB_UNIT_TEST_CASE(InMemTriValueSkipListReaderTest, TestCaseForGetPrevAndCurrentTTF);

void InMemTriValueSkipListReaderTest::TestCaseForGetPrevAndCurrentTTF()
{
    InnerTestGetPrevAndCurrentTTF(10, true);
    InnerTestGetPrevAndCurrentTTF(32, true);
    InnerTestGetPrevAndCurrentTTF(33, true);

    InnerTestGetPrevAndCurrentTTF(10, false);
    InnerTestGetPrevAndCurrentTTF(32, false);
    InnerTestGetPrevAndCurrentTTF(33, false);
}

SkipListReaderPtr InMemTriValueSkipListReaderTest::PrepareReader(
        uint32_t itemCount, bool needFlush)
{
    DocListFormatOption option(OPTION_FLAG_ALL);
    mFormat.reset(new DocListSkipListFormat(option));
    mWriter.reset(new BufferedSkipListWriter(mByteSlicePool, mBufferPool));
    mWriter->Init(mFormat.get());

    mWriter->AddItem(1, 5, 0);
    for (uint32_t i = 1; i < itemCount; ++i)
    {
        mWriter->AddItem(2 * i + 1, 5 * i + 1, 3);
    }
    if (needFlush)
    {
        mWriter->FinishFlush();
    }

    InMemTriValueSkipListReaderPtr reader(
            new InMemTriValueSkipListReader(mByteSlicePool));
    reader->Load(mWriter.get());

    return reader;
}

IE_NAMESPACE_END(index);

