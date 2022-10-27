#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/expandable_bitmap.cpp"

using namespace std;

IE_NAMESPACE_BEGIN(util);

class ExpandableBitmapTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ExpandableBitmapTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForExpand()
    {
        ExpandableBitmap exBitmap(200, false);
        exBitmap.Set(100);
        exBitmap.Set(400);

        INDEXLIB_TEST_TRUE(exBitmap.Test(100));
        INDEXLIB_TEST_TRUE(!exBitmap.Test(399));
        INDEXLIB_TEST_TRUE(exBitmap.Test(400));
    }

    void TestCaseForExpandWithSmallSlot()
    {
        ExpandableBitmap exBitmap(10, false);
        exBitmap.Set(100);

        INDEXLIB_TEST_TRUE(!exBitmap.Test(9));
        INDEXLIB_TEST_TRUE(exBitmap.Test(100));
    }

    void TestCaseForValidItemCount()
    {
        ExpandableBitmap exBitmap(200, false);
        INDEXLIB_TEST_EQUAL((uint32_t)200, exBitmap.GetValidItemCount());

        exBitmap.Set(100);
        INDEXLIB_TEST_TRUE(exBitmap.Test(100));
        INDEXLIB_TEST_EQUAL((uint32_t)200, exBitmap.GetValidItemCount());

        exBitmap.Set(400);
        INDEXLIB_TEST_TRUE(exBitmap.Test(400));
        INDEXLIB_TEST_EQUAL((uint32_t)416, exBitmap.GetValidItemCount());

        exBitmap.Set(199);
        INDEXLIB_TEST_EQUAL((uint32_t)416, exBitmap.GetValidItemCount());

        exBitmap.Reset(199);
        INDEXLIB_TEST_EQUAL((uint32_t)416, exBitmap.GetValidItemCount());

        exBitmap.Reset(400);
        INDEXLIB_TEST_EQUAL((uint32_t)416, exBitmap.GetValidItemCount());

        INDEXLIB_TEST_TRUE(!exBitmap.Test(399));

        exBitmap.ReSize(500);
        INDEXLIB_TEST_EQUAL((uint32_t)608, exBitmap.GetValidItemCount());

        exBitmap.ReSize(300);
        INDEXLIB_TEST_EQUAL((uint32_t)300, exBitmap.GetValidItemCount());

    }

    void TestCaseForValidItemCountWithSetTrue()
    {
        ExpandableBitmap exBitmap(200, true);
        INDEXLIB_TEST_EQUAL((uint32_t)200, exBitmap.GetValidItemCount());

        exBitmap.Set(100);
        INDEXLIB_TEST_TRUE(exBitmap.Test(100));
        INDEXLIB_TEST_EQUAL((uint32_t)200, exBitmap.GetValidItemCount());

        exBitmap.Set(400);
        INDEXLIB_TEST_TRUE(exBitmap.Test(400));
        INDEXLIB_TEST_EQUAL((uint32_t)416, exBitmap.GetValidItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)13, exBitmap.GetSlotCount());

        exBitmap.ReSize(401);

        INDEXLIB_TEST_EQUAL((uint32_t)401, exBitmap.GetValidItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)13, exBitmap.GetSlotCount());
    }

    void TestCloneWithOnlyValidDataAfterResize()
    {
        ExpandableBitmap exBitmap(200, false);
        INDEXLIB_TEST_EQUAL((uint32_t)200, exBitmap.GetValidItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)7, exBitmap.GetSlotCount());

        exBitmap.Set(100);
        INDEXLIB_TEST_TRUE(exBitmap.Test(100));
        INDEXLIB_TEST_TRUE(!exBitmap.Test(50));
        INDEXLIB_TEST_EQUAL((uint32_t)200, exBitmap.GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)200, exBitmap.GetValidItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)1, exBitmap.GetSetCount());

        exBitmap.ReSize(101);
        
        ExpandableBitmap* clonedExBitmap = exBitmap.CloneWithOnlyValidData();
        ExpandableBitmapPtr clonedExBitmapPtr(clonedExBitmap);

        INDEXLIB_TEST_EQUAL((uint32_t)101, clonedExBitmap->GetValidItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)101, clonedExBitmap->GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)1, clonedExBitmap->GetSetCount());
        INDEXLIB_TEST_TRUE(clonedExBitmap->Test(100));
        INDEXLIB_TEST_TRUE(!clonedExBitmap->Test(50));
        INDEXLIB_TEST_EQUAL((uint32_t)4, clonedExBitmap->GetSlotCount());
    }

    void TestCloneWithOnlyValidDataWithoutResize()
    {
        ExpandableBitmap exBitmap(200, false);
        INDEXLIB_TEST_EQUAL((uint32_t)7, exBitmap.GetSlotCount());        

        exBitmap.Set(100);
        INDEXLIB_TEST_TRUE(exBitmap.Test(100));
        INDEXLIB_TEST_TRUE(!exBitmap.Test(50));
        INDEXLIB_TEST_EQUAL((uint32_t)200, exBitmap.GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)200, exBitmap.GetValidItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)1, exBitmap.GetSetCount());
        
        ExpandableBitmap* clonedExBitmap = exBitmap.CloneWithOnlyValidData();
        ExpandableBitmapPtr clonedExBitmapPtr(clonedExBitmap);

        INDEXLIB_TEST_EQUAL((uint32_t)200, clonedExBitmap->GetValidItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)200, clonedExBitmap->GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)1, clonedExBitmap->GetSetCount());
        INDEXLIB_TEST_TRUE(clonedExBitmap->Test(100));
        INDEXLIB_TEST_TRUE(!clonedExBitmap->Test(50));
        INDEXLIB_TEST_EQUAL((uint32_t)7, clonedExBitmap->GetSlotCount());
    }


private:
};

INDEXLIB_UNIT_TEST_CASE(ExpandableBitmapTest, TestCaseForExpand);
INDEXLIB_UNIT_TEST_CASE(ExpandableBitmapTest, TestCaseForExpandWithSmallSlot);
INDEXLIB_UNIT_TEST_CASE(ExpandableBitmapTest, TestCaseForValidItemCount);
INDEXLIB_UNIT_TEST_CASE(ExpandableBitmapTest, TestCaseForValidItemCountWithSetTrue);
INDEXLIB_UNIT_TEST_CASE(ExpandableBitmapTest, TestCloneWithOnlyValidDataAfterResize);
INDEXLIB_UNIT_TEST_CASE(ExpandableBitmapTest, TestCloneWithOnlyValidDataWithoutResize);

IE_NAMESPACE_END(util);
