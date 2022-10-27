#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/test/skip_list_reader_unittest_base.h"

IE_NAMESPACE_BEGIN(index);

class TriValueSkipListReaderTest : public SkipListReaderTestBase
{
public:
    TriValueSkipListReaderTest();
    virtual ~TriValueSkipListReaderTest();

    DECLARE_CLASS_NAME(TriValueSkipListReaderTest); 

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestCaseForGetPrevAndCurrentTTF();

protected:
    virtual SkipListReaderPtr PrepareReader(uint32_t itemCount, bool needFlush) override;
    void InnerTestGetPrevAndCurrentTTF(uint32_t itemCount, bool needFlush);


protected:
    common::ByteSliceWriter* mByteSliceWriter;
    uint32_t mStart;
    uint32_t mEnd;

private:
    std::string mDir;
};

IE_NAMESPACE_END(index);
