#include "indexlib/index/normal/inverted_index/format/skiplist/test/skip_list_reader_unittest.h"
#include "indexlib/common/byte_slice_writer.h"

using namespace autil::legacy;
using namespace autil;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SkipListReaderTest);

SkipListReaderTest::SkipListReaderTest()
{
}

SkipListReaderTest::~SkipListReaderTest()
{
}

void SkipListReaderTest::CaseSetUp()
{
}

void SkipListReaderTest::CaseTearDown()
{
}

void SkipListReaderTest::TestSimpleProcess()
{
    SkipListReader reader;
    ByteSliceWriter byteWriter;
    const uint32_t ITEM_COUNT = 100;
    for (uint32_t i = 0; i < ITEM_COUNT; ++i)
    {
        byteWriter.WriteUInt32(i);
    }
    reader.Load(byteWriter.GetByteSliceList(), 0, byteWriter.GetSize());
        
    ByteSliceReader& byteReader = reader.mByteSliceReader;

    uint32_t value;
    for (uint32_t i = 0; i < ITEM_COUNT; ++i)
    {
        value = byteReader.ReadUInt32();
        INDEXLIB_TEST_EQUAL(value, i);
    }
}

IE_NAMESPACE_END(index);

