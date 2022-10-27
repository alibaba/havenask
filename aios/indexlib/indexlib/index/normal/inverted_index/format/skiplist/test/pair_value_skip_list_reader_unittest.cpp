#include "indexlib/index/normal/inverted_index/format/skiplist/test/pair_value_skip_list_reader_unittest.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/buffered_skip_list_writer.h"
#include <autil/mem_pool/SimpleAllocatePolicy.h>
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_skip_list_format.h"

using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);

INDEXLIB_UNIT_TEST_CASE(PairValueSkipListReaderTest, TestCaseForSkipToNotEqual);
INDEXLIB_UNIT_TEST_CASE(PairValueSkipListReaderTest, TestCaseForSkipToEqual);
INDEXLIB_UNIT_TEST_CASE(PairValueSkipListReaderTest, TestCaseForGetSkippedItemCount);
INDEXLIB_UNIT_TEST_CASE(PairValueSkipListReaderTest, TestCaseForGetLastValueInBuffer);
INDEXLIB_UNIT_TEST_CASE(PairValueSkipListReaderTest, TestCaseForGetLastValueInBufferForSeekFailed);


PairValueSkipListReaderTest::PairValueSkipListReaderTest()
{
    mByteSliceWriter = new ByteSliceWriter(mByteSlicePool);
}

PairValueSkipListReaderTest::~PairValueSkipListReaderTest()
{
    delete mByteSliceWriter;
    mByteSliceWriter = NULL;
}

SkipListReaderPtr PairValueSkipListReaderTest::PrepareReader(
        uint32_t itemCount, bool needFlush)
{
    PairValueSkipListReaderPtr reader(new PairValueSkipListReader);
    
    mByteSliceWriter->Reset();
    for (uint32_t i = 0; i < itemCount; ++i)
    {
        mByteSliceWriter->WriteUInt32(i);
    }

    mStart = mByteSliceWriter->GetSize();

    DocListFormatOption option(NO_TERM_FREQUENCY);
    DocListSkipListFormat format(option);
    BufferedSkipListWriter writer(mByteSlicePool, mBufferPool);
    writer.Init(&format);        

    writer.AddItem(1, 0);
    for (uint32_t i = 1; i < itemCount; ++i)
    {
        writer.AddItem(2 * i + 1, 3);
    }
        
    writer.FinishFlush();

    mByteSliceWriter->Write(*(writer.GetByteSliceList()));
    mEnd = mByteSliceWriter->GetSize();    

    reader->Load(mByteSliceWriter->GetByteSliceList(), mStart, mEnd, itemCount);

    return reader;
}

IE_NAMESPACE_END(index);
