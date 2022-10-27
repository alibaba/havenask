#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

class SegmentTermInfoTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SegmentTermInfoTest);
    void CaseSetUp() override
    {
        mItem1 = new SegmentTermInfo(0, 0, IndexIteratorPtr());
        mItem2 = new SegmentTermInfo(1, 0, IndexIteratorPtr());
    }

    void CaseTearDown() override
    {
        delete mItem1;
        delete mItem2;
    }

    void TestCaseForSegmentTermInfoComparator()
    {
        InnerTest<int8_t>(1, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, false);

        InnerTest<uint8_t>(1, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, false);
        InnerTest<uint8_t>(255, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, true);        

        InnerTest<int16_t>(32767, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, true);        

        InnerTest<uint16_t>(0, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, false);
        InnerTest<uint16_t>(65535, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, true);        

        InnerTest<uint16_t>(0, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, false);
        InnerTest<uint16_t>(65535, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, true);        

        InnerTest<int32_t>(65535, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, true);        

        InnerTest<uint32_t>(0, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, false);
        InnerTest<uint32_t>(65535, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, true);        

        InnerTest<int64_t>(65535, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, true);        

        InnerTest<uint64_t>(0, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, false);
        InnerTest<uint64_t>(65535, 127, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_NORMAL, true);

        InnerTest<uint64_t>(65535, 65535, SegmentTermInfo::TM_NORMAL, SegmentTermInfo::TM_BITMAP, false);

        InnerTest<uint64_t>(65535, 65535, SegmentTermInfo::TM_BITMAP, SegmentTermInfo::TM_BITMAP, false);
    }


private:
    template <typename KeyType>
    void InnerTest(KeyType key1, KeyType key2, 
                   SegmentTermInfo::TermIndexMode mode1, 
                   SegmentTermInfo::TermIndexMode mode2,
                   bool expectedResult)
    {
        mItem1->key = (dictkey_t)key1;
        mItem2->key = (dictkey_t)key2;
        mItem1->termIndexMode = mode1;
        mItem2->termIndexMode = mode2;
        Check(mItem1, mItem2, expectedResult);
    }

    void Check(const SegmentTermInfo *item1, 
               const SegmentTermInfo *item2, 
               bool expectedResult) 
    {
        SegmentTermInfoComparator comparator;
        bool result = comparator(item1, item2);
        INDEXLIB_TEST_EQUAL(expectedResult, result);
    }

private:
    SegmentTermInfo *mItem1;
    SegmentTermInfo *mItem2;
};

INDEXLIB_UNIT_TEST_CASE(SegmentTermInfoTest, TestCaseForSegmentTermInfoComparator);

IE_NAMESPACE_END(index);
