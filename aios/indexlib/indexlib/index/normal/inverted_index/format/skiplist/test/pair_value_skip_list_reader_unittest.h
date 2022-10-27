#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/pair_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/test/skip_list_reader_unittest_base.h"

IE_NAMESPACE_BEGIN(index);

class PairValueSkipListReaderTest : public SkipListReaderTestBase
{
public:
    PairValueSkipListReaderTest();
    ~PairValueSkipListReaderTest();

    DECLARE_CLASS_NAME(PairValueSkipListReaderTest);
protected:
    SkipListReaderPtr PrepareReader(uint32_t itemCount, bool needFlush) override;

protected:
    common::ByteSliceWriter* mByteSliceWriter;
    uint32_t mStart;
    uint32_t mEnd;
};

IE_NAMESPACE_END(index);
