#include "indexlib/index/normal/inverted_index/format/skiplist/test/in_mem_pair_value_skip_list_reader_unittest.h"
#include <autil/mem_pool/SimpleAllocatePolicy.h>
#include "indexlib/index_define.h"


using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InMemPairValueSkipListReaderTest);

INDEXLIB_UNIT_TEST_CASE(InMemPairValueSkipListReaderTest, TestCaseForSkipToNotEqual);
INDEXLIB_UNIT_TEST_CASE(InMemPairValueSkipListReaderTest, TestCaseForSkipToEqual);
INDEXLIB_UNIT_TEST_CASE(InMemPairValueSkipListReaderTest, TestCaseForGetSkippedItemCount);
INDEXLIB_UNIT_TEST_CASE(InMemPairValueSkipListReaderTest, TestCaseForGetLastValueInBuffer);
INDEXLIB_UNIT_TEST_CASE(InMemPairValueSkipListReaderTest, TestCaseForGetLastValueInBufferForSeekFailed);
INDEXLIB_UNIT_TEST_CASE(InMemPairValueSkipListReaderTest, TestCaseForSkipToNotEqualWithFlush);
INDEXLIB_UNIT_TEST_CASE(InMemPairValueSkipListReaderTest, TestCaseForSkipToEqualWithFlush);
INDEXLIB_UNIT_TEST_CASE(InMemPairValueSkipListReaderTest, TestCaseForGetSkippedItemCountWithFlush);
INDEXLIB_UNIT_TEST_CASE(InMemPairValueSkipListReaderTest, TestCaseForGetLastValueInBufferForFlush);

SkipListReaderPtr InMemPairValueSkipListReaderTest::PrepareReader(
        uint32_t itemCount, bool needFlush)
{
    DocListFormatOption option(NO_TERM_FREQUENCY);
    mFormat.reset(new DocListSkipListFormat(option));
    mWriter.reset(new BufferedSkipListWriter(mByteSlicePool, mBufferPool));
    mWriter->Init(mFormat.get());        

    mWriter->AddItem(1, 0);
    for (uint32_t i = 1; i < itemCount; ++i)
    {
        mWriter->AddItem(2 * i + 1, 3);
    }

    if (needFlush)
    {
        mWriter->FinishFlush();
    }

    InMemPairValueSkipListReaderPtr reader(
            new InMemPairValueSkipListReader(mByteSlicePool, false));
    reader->Load(mWriter.get());

    return reader;
}

IE_NAMESPACE_END(index);

