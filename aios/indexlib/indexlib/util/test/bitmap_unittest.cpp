#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/bitmap.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);

class BitmapTest : public INDEXLIB_TESTBASE
{
private:
    Bitmap* mBitmap;

public:
    typedef bool (*ShouldSet) (uint32_t idx);


    static bool SetNone(uint32_t idx)
    {
        return false;
    }

    static bool SetAll(uint32_t idx)
    {
        return true;
    }

    static bool SetEven(uint32_t idx)
    {
        return idx % 2 == 0;
    }

    static bool SetOdd(uint32_t idx)
    {
        return idx % 2 == 1;
    }

public:
    DECLARE_CLASS_NAME(BitmapTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void MySetup()
    {
        mBitmap = new Bitmap;
    }

    void MyCleanUp()
    {
        delete mBitmap;
    }

    void TestCaseForAllocAllReset() 
    {
        MySetup();
        bool ret = mBitmap->Alloc(33);
        INDEXLIB_TEST_TRUE(ret);
        INDEXLIB_TEST_EQUAL((uint32_t)33, mBitmap->GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)33, mBitmap->GetUnsetCount());
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForAllocAllSet() 
    {
        MySetup();
        bool ret = mBitmap->Alloc(33, true);
        INDEXLIB_TEST_TRUE(ret);
        INDEXLIB_TEST_EQUAL((uint32_t)33, mBitmap->GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)33, mBitmap->GetSetCount());
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetUnsetCount());
        MyCleanUp();
    }

    void TestCaseForSetAndTest()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        INDEXLIB_TEST_TRUE(!mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(100));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));

        mBitmap->Set(0);
        INDEXLIB_TEST_TRUE(mBitmap->Test(0));

        mBitmap->Set(100);
        INDEXLIB_TEST_TRUE(mBitmap->Test(100));

        mBitmap->Set(332);
        INDEXLIB_TEST_TRUE(mBitmap->Test(332));

        for (uint32_t i = 0; i < 333; ++i)
        {
            if (i!= 0 && i!= 100 && i!= 332)
            {
                INDEXLIB_TEST_TRUE(!mBitmap->Test(i));
            }
        }
        MyCleanUp();
    }

    void TestCaseForReAlloc()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);

        mBitmap->Set(0);
        mBitmap->Set(100);
        mBitmap->Set(332);

        mBitmap->Alloc(400);
        INDEXLIB_TEST_TRUE(!mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(100));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));
        
        MyCleanUp();
    }

    void TestCaseForReset()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333, true);
        INDEXLIB_TEST_TRUE(ret);
    
        INDEXLIB_TEST_TRUE(mBitmap->Test(0));
        mBitmap->Reset(0);
        INDEXLIB_TEST_TRUE(!mBitmap->Test(0));

        INDEXLIB_TEST_TRUE(mBitmap->Test(100));
        mBitmap->Reset(100);
        INDEXLIB_TEST_TRUE(!mBitmap->Test(100));

        INDEXLIB_TEST_TRUE(mBitmap->Test(332));
        mBitmap->Reset(332);
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));
        MyCleanUp();
    }

    void TestCaseForResetAll()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333, true);
        INDEXLIB_TEST_TRUE(ret);

        mBitmap->ResetAll();
    
        INDEXLIB_TEST_TRUE(!mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(100));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));
        MyCleanUp();
    }

    void TestCaseForResetAllMore()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);

        mBitmap->Set(0);
        mBitmap->Set(100);
        mBitmap->Set(332);
        mBitmap->ResetAll();
    
        INDEXLIB_TEST_TRUE(!mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(100));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(222));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));
    
        INDEXLIB_TEST_EQUAL((uint32_t)333, mBitmap->GetUnsetCount());
        MyCleanUp();
    }
    void TestCaseForResetAllAfterHead()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333, true);
        INDEXLIB_TEST_TRUE(ret);

        mBitmap->ResetAllAfter(0);
        INDEXLIB_TEST_TRUE(!mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(100));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(222));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));
    
        INDEXLIB_TEST_EQUAL((uint32_t)333, mBitmap->GetUnsetCount());
        MyCleanUp();
    }

    void TestCaseForResetAllAfterMiddle()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333, true);
        INDEXLIB_TEST_TRUE(ret);

        mBitmap->ResetAllAfter(100);
        INDEXLIB_TEST_TRUE(mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(mBitmap->Test(99));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(100));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(101));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(222));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));
    
        INDEXLIB_TEST_EQUAL((uint32_t)233, mBitmap->GetUnsetCount());
        MyCleanUp();
    }

    void TestCaseForResetAllAfterTail()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333, true);
        INDEXLIB_TEST_TRUE(ret);

        mBitmap->ResetAllAfter(332);
        INDEXLIB_TEST_TRUE(mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(mBitmap->Test(100));
        INDEXLIB_TEST_TRUE(mBitmap->Test(222));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));
    
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetUnsetCount());
        MyCleanUp();
    }

    void TestCaseForGetItemCount()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333, true);
        INDEXLIB_TEST_TRUE(ret);
        mBitmap->Reset(200);
        INDEXLIB_TEST_EQUAL((uint32_t)333, mBitmap->GetItemCount());
        MyCleanUp();
    }

    void TestCaseForSize()
    {
        MySetup();
        bool ret = mBitmap->Alloc(320, true);
        INDEXLIB_TEST_TRUE(ret);
        INDEXLIB_TEST_EQUAL((uint32_t)40, mBitmap->Size());
        MyCleanUp();
    }

    void TestCaseForSizeOnBitMore()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333, true);
        INDEXLIB_TEST_TRUE(ret);
        INDEXLIB_TEST_EQUAL((uint32_t)44, mBitmap->Size());
        MyCleanUp();
    }

    void TestCaseForGetSetCount()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333, true);
        INDEXLIB_TEST_TRUE(ret);
        mBitmap->Reset(200);
        INDEXLIB_TEST_EQUAL((uint32_t)332, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForGetSetCountWithRange()
    {
        MySetup();
        mBitmap->Alloc(320);
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(0, 32));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(1, 31));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(1, 40));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(1, 320));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(321, 320));
        
        mBitmap->Set(3);
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(0, 32));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(0, 3));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(1, 3));

        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(3, 3));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(3, 31));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(3, 32));

        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(4, 32));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(1, 31));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(4, 31));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(1, 40));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(4, 40));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(1, 320));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(4, 320));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(321, 320));

        mBitmap->Set(32);
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(0, 31));
        INDEXLIB_TEST_EQUAL((uint32_t)2, mBitmap->GetSetCount(0, 32));

        INDEXLIB_TEST_EQUAL((uint32_t)2, mBitmap->GetSetCount(3, 32));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(4, 32));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(4, 40));

        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(32, 32));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(32, 40));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(32, 320));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(33, 320));

        mBitmap->Set(29);
        INDEXLIB_TEST_EQUAL((uint32_t)2, mBitmap->GetSetCount(0, 31));
        INDEXLIB_TEST_EQUAL((uint32_t)2, mBitmap->GetSetCount(0, 30));        
        INDEXLIB_TEST_EQUAL((uint32_t)2, mBitmap->GetSetCount(0, 29));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount(0, 28));
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount(4, 28));

        mBitmap->Set(40);
        mBitmap->Set(80);
        INDEXLIB_TEST_EQUAL((uint32_t)2, mBitmap->GetSetCount(0, 31));
        INDEXLIB_TEST_EQUAL((uint32_t)2, mBitmap->GetSetCount(2, 31));
        INDEXLIB_TEST_EQUAL((uint32_t)3, mBitmap->GetSetCount(0, 32));
        INDEXLIB_TEST_EQUAL((uint32_t)3, mBitmap->GetSetCount(2, 32));
        INDEXLIB_TEST_EQUAL((uint32_t)4, mBitmap->GetSetCount(0, 40));
        INDEXLIB_TEST_EQUAL((uint32_t)4, mBitmap->GetSetCount(2, 40));
        INDEXLIB_TEST_EQUAL((uint32_t)4, mBitmap->GetSetCount(0, 79));
        INDEXLIB_TEST_EQUAL((uint32_t)4, mBitmap->GetSetCount(2, 79));
        INDEXLIB_TEST_EQUAL((uint32_t)5, mBitmap->GetSetCount(0, 80));
        INDEXLIB_TEST_EQUAL((uint32_t)5, mBitmap->GetSetCount(2, 80));
        INDEXLIB_TEST_EQUAL((uint32_t)5, mBitmap->GetSetCount(0, 320));
        INDEXLIB_TEST_EQUAL((uint32_t)5, mBitmap->GetSetCount(2, 320));
        
        MyCleanUp();
    }

    void TestCaseForMountAllReset()
    {
        MySetup();
        uint32_t *data = new uint32_t[11];
        memset(data, 0, 11 * sizeof(uint32_t));
        mBitmap->Mount(11 * sizeof(uint32_t), data, false);
        mBitmap->RefreshSetCountByScanning();

        INDEXLIB_TEST_EQUAL((uint32_t)(11 * sizeof(uint32_t)), mBitmap->GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForMountPartSet()
    {
        MySetup();
        uint32_t *data = new uint32_t[11];
        memset(data, 0, 11 * sizeof(uint32_t));
        data[1] = 0xFFFFFFFF;
        mBitmap->Mount(11 * 8 * sizeof(uint32_t), data, false);
        mBitmap->RefreshSetCountByScanning();

        INDEXLIB_TEST_EQUAL((uint32_t)(11 * 8 * sizeof(uint32_t)), mBitmap->GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)32, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorSquareBracket()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        INDEXLIB_TEST_EQUAL((uint32_t)0, (*mBitmap)[0]);
        INDEXLIB_TEST_EQUAL((uint32_t)0, (*mBitmap)[5]);
        INDEXLIB_TEST_EQUAL((uint32_t)0, (*mBitmap)[10]);

        mBitmap->Set(0);
        INDEXLIB_TEST_EQUAL((uint32_t)0x80000000, (*mBitmap)[0]);
        mBitmap->Set(5*32 - 1);
        INDEXLIB_TEST_EQUAL((uint32_t)0x00000001, (*mBitmap)[4]);
        mBitmap->Set(5*32 + 2);
        INDEXLIB_TEST_EQUAL((uint32_t)0x20000000, (*mBitmap)[5]);
        mBitmap->Set(332);
        INDEXLIB_TEST_EQUAL((uint32_t)0x00080000, (*mBitmap)[10]);
        MyCleanUp();
    }

    void TestCaseForOperatorAndTowAllReset()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        Bitmap &bitmapRes = (*mBitmap) & bitmap2;
        INDEXLIB_TEST_EQUAL((uint32_t)0, bitmapRes.GetSetCount());
        MyCleanUp();
    }
    void TestCaseForOperatorAnd()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        mBitmap->Set(0);
        mBitmap->Set(200);
        INDEXLIB_TEST_TRUE(ret);

        Bitmap bitmap2((uint32_t)333);
        bitmap2.Set(332);
        bitmap2.Set(200);

        Bitmap &bitmapRes = (*mBitmap) & bitmap2;
        INDEXLIB_TEST_TRUE(!bitmapRes.Test(0));
        INDEXLIB_TEST_TRUE(bitmapRes.Test(200));
        INDEXLIB_TEST_TRUE(!bitmapRes.Test(332));

        INDEXLIB_TEST_EQUAL((uint32_t)1, bitmapRes.GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorAndEqualTowAllReset()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        (*mBitmap) &= bitmap2;
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorAndEqual()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        mBitmap->Set(0);
        mBitmap->Set(200);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        bitmap2.Set(332);
        bitmap2.Set(200);
        (*mBitmap) &= bitmap2;
        INDEXLIB_TEST_TRUE(!mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(mBitmap->Test(200));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorOrTowAllReset()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        Bitmap &bitmapRes = (*mBitmap) | bitmap2;
        INDEXLIB_TEST_EQUAL((uint32_t)0, bitmapRes.GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorOr()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        mBitmap->Set(0);
        mBitmap->Set(200);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        bitmap2.Set(332);
        bitmap2.Set(200);
        Bitmap &bitmapRes = (*mBitmap) | bitmap2;
        INDEXLIB_TEST_TRUE(bitmapRes.Test(0));
        INDEXLIB_TEST_TRUE(bitmapRes.Test(200));
        INDEXLIB_TEST_TRUE(bitmapRes.Test(332));
        INDEXLIB_TEST_EQUAL((uint32_t)3, bitmapRes.GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorOrEqualTowAllReset()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        (*mBitmap) |= bitmap2;
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorOrEqual()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        mBitmap->Set(0);
        mBitmap->Set(200);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        bitmap2.Set(332);
        bitmap2.Set(200);
        (*mBitmap) |= bitmap2;
        INDEXLIB_TEST_TRUE(mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(mBitmap->Test(200));
        INDEXLIB_TEST_TRUE(mBitmap->Test(332));
        INDEXLIB_TEST_EQUAL((uint32_t)3, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorNotAllReset()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap &bitmapRes = ~(*mBitmap);
        INDEXLIB_TEST_EQUAL((uint32_t)333, bitmapRes.GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorNot()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        mBitmap->Set(0);
        mBitmap->Set(200);
        mBitmap->Set(332);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap &bitmapRes = ~(*mBitmap);
        INDEXLIB_TEST_TRUE(!bitmapRes.Test(0));
        INDEXLIB_TEST_TRUE(!bitmapRes.Test(200));
        INDEXLIB_TEST_TRUE(!bitmapRes.Test(332));
        INDEXLIB_TEST_EQUAL((uint32_t)330, bitmapRes.GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorMinusAllReset()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        Bitmap &bitmapRes = (*mBitmap) - bitmap2;
        INDEXLIB_TEST_EQUAL((uint32_t)0, bitmapRes.GetSetCount());
        MyCleanUp();
    }
    void TestCaseForOperatorMinus()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        mBitmap->Set(0);
        mBitmap->Set(200);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        bitmap2.Set(332);
        bitmap2.Set(200);
        Bitmap &bitmapRes = (*mBitmap) - bitmap2;
        INDEXLIB_TEST_TRUE(bitmapRes.Test(0));
        INDEXLIB_TEST_TRUE(!bitmapRes.Test(200));
        INDEXLIB_TEST_TRUE(!bitmapRes.Test(332));
        INDEXLIB_TEST_EQUAL((uint32_t)1, bitmapRes.GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorMinusEqualAllReset()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        (*mBitmap) -= bitmap2;
        INDEXLIB_TEST_EQUAL((uint32_t)0, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForOperatorMinusEqual()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        mBitmap->Set(0);
        mBitmap->Set(200);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2((uint32_t)333);
        bitmap2.Set(332);
        bitmap2.Set(200);
        (*mBitmap) -= bitmap2;
        INDEXLIB_TEST_TRUE(mBitmap->Test(0));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(200));
        INDEXLIB_TEST_TRUE(!mBitmap->Test(332));
        INDEXLIB_TEST_EQUAL((uint32_t)1, mBitmap->GetSetCount());
        MyCleanUp();
    }

    void TestCaseForEvaluateAllReset()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2 = *mBitmap;
    
        INDEXLIB_TEST_EQUAL((uint32_t)44, bitmap2.Size());
        INDEXLIB_TEST_EQUAL((uint32_t)333, bitmap2.GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)0, bitmap2.GetSetCount());
        MyCleanUp();
    }

    void TestCaseForEvaluate()
    {
        MySetup();
        bool ret = mBitmap->Alloc(333);
        mBitmap->Set(0);
        mBitmap->Set(200);
        INDEXLIB_TEST_TRUE(ret);
        Bitmap bitmap2 = *mBitmap;
        INDEXLIB_TEST_TRUE(bitmap2.Test(0));
        INDEXLIB_TEST_TRUE(bitmap2.Test(200));
        INDEXLIB_TEST_TRUE(!bitmap2.Test(332));
    
        INDEXLIB_TEST_EQUAL((uint32_t)44, bitmap2.Size());
        INDEXLIB_TEST_EQUAL((uint32_t)333, bitmap2.GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)2, bitmap2.GetSetCount());
        MyCleanUp();
    }

    void TestCaseForHasSetData()
    {
        const uint32_t ITEM_NUM = 1024;

        Bitmap bitmap(ITEM_NUM, false);
        for (uint32_t i = sizeof(uint32_t); i < ITEM_NUM - sizeof(uint32_t); ++i)
        {
            bitmap.Set(i);

            INDEXLIB_TEST_TRUE(bitmap.HasSetData(0, ITEM_NUM - 1));
            INDEXLIB_TEST_TRUE(bitmap.HasSetData(0, i));
            INDEXLIB_TEST_TRUE(!bitmap.HasSetData(0, i - 1));
            INDEXLIB_TEST_TRUE(bitmap.HasSetData(i, ITEM_NUM - 1));
            INDEXLIB_TEST_TRUE(!bitmap.HasSetData(i + 1, ITEM_NUM - 1));
            assert(!bitmap.HasSetData(i + 1, ITEM_NUM - 1));

            bitmap.Reset(i);
            INDEXLIB_TEST_TRUE(!bitmap.HasSetData(0, ITEM_NUM - 1));
            INDEXLIB_TEST_TRUE(!bitmap.HasSetData(0, i));
            INDEXLIB_TEST_TRUE(!bitmap.HasSetData(0, i - 1));
            INDEXLIB_TEST_TRUE(!bitmap.HasSetData(i, ITEM_NUM - 1));
            INDEXLIB_TEST_TRUE(!bitmap.HasSetData(i + 1, ITEM_NUM - 1));
        }
    }

    void TestCaseForResetBetween()
    {
        const uint32_t ITEM_NUM = 128;

        for (uint32_t i = sizeof(uint32_t); i < ITEM_NUM - sizeof(uint32_t); ++i)
        {
            for (uint32_t j = i + 1; j < ITEM_NUM - 1; ++j)
            {
                Bitmap bitmap(ITEM_NUM, true);
                bitmap.ResetBetween(i, j);
                INDEXLIB_TEST_TRUE(!bitmap.HasSetData(i, j));
                INDEXLIB_TEST_TRUE(bitmap.HasSetData(i, j + 1));
                INDEXLIB_TEST_TRUE(bitmap.HasSetData(i - 1, j));
            }
        }
    }

    void TestCaseForCopy()
    {
        srand(time(NULL));
        const uint32_t BITMAP_NUM = 200;
        vector<Bitmap*> bitmaps;
        uint32_t totalItemCount = CreateBitmap(bitmaps, BITMAP_NUM);
        Bitmap *mergedBitmap = new Bitmap(totalItemCount);

        // test all zeros
        InnerTestForCopy(bitmaps, mergedBitmap, SetNone);
        // test all ones
        InnerTestForCopy(bitmaps, mergedBitmap, SetAll);
        // test even ones
        InnerTestForCopy(bitmaps, mergedBitmap, SetEven);
        // test odd ones
        InnerTestForCopy(bitmaps, mergedBitmap, SetOdd);
        // test random
        InnerTestForCopy(bitmaps, mergedBitmap, NULL, true);

        delete mergedBitmap;
        for (size_t i = 0; i < bitmaps.size(); i++)
        {
            delete bitmaps[i];
        }
    }

    void InnerTestForCopy(const vector<Bitmap*> &bitmaps, 
                            Bitmap *mergedBitmap, 
                            ShouldSet shouldSet,
                            bool random = false)
    {
        mergedBitmap->ResetAll();
        uint32_t startIndex = 0;
        for (size_t i = 0; i < bitmaps.size(); i++)
        {
            Bitmap *bitmap = bitmaps[i];
            bitmap->ResetAll();
            uint32_t itemCount = bitmap->GetItemCount();
            for (uint32_t j = 0; j < itemCount; j++)
            {
                if (random)
                {
                    bitmap->Set(rand() % itemCount);
                }
                else
                {
                    assert(shouldSet != NULL);
                    if (shouldSet(j))
                    {
                        bitmap->Set(j);
                    }
                }
            }
            mergedBitmap->Copy(startIndex, bitmap->GetData(), itemCount);
            startIndex += itemCount;
        }
        CheckAnswer(bitmaps, mergedBitmap);
    }

    uint32_t CreateBitmap(vector<Bitmap*> &bitmaps, uint32_t count)
    {
        uint32_t totalItemCount = 0;
        for (uint32_t i = 0; i < count; i++)
        {
            uint32_t itemCount = rand() % 4096 + 1;
            Bitmap *bitmap = new Bitmap(itemCount);
            bitmaps.push_back(bitmap);
            totalItemCount += itemCount;            
        }
        return totalItemCount;
    }

    void CheckAnswer(const vector<Bitmap*> &bitmaps, Bitmap *mergedBitmap)
    {
        uint32_t startIndex = 0;
        uint32_t totalSetCount = 0;
        for (size_t i = 0; i < bitmaps.size(); i++)
        {
            Bitmap *bitmap = bitmaps[i];
            uint32_t itemCount = bitmap->GetItemCount();
            totalSetCount += bitmap->GetSetCount();
            for (uint32_t j = 0; j < itemCount; j++)
            {
                bool expected = bitmap->Test(j);
                bool actual = mergedBitmap->Test(startIndex + j);
                INDEXLIB_TEST_EQUAL(expected, actual);
            }
            startIndex += itemCount;
        }
        INDEXLIB_TEST_EQUAL(totalSetCount, mergedBitmap->GetSetCount());
    }

    void TestCaseForCopySlice()
    {
        MySetup();
        const uint32_t ITEM_COUNT = 11111;
        mBitmap->Alloc(ITEM_COUNT);
        for (uint32_t i = 0; i < ITEM_COUNT; i++)
        {
            if ((i & 1) == 0) 
            {
                mBitmap->Set(i);
            }
        }
        INDEXLIB_TEST_EQUAL(ITEM_COUNT / 2 + 1, mBitmap->GetSetCount());
        for (int i = 0; i < 128; ++i) {
            for (int j = 0; j < 128; ++j) {
                InnerTestForCopySlice2(i, j);
            } 
        }
        // copy nothing
        InnerTestForCopySlice(mBitmap, 0, 0, 0);
        // one slot in dst can hold copied bits
        InnerTestForCopySlice(mBitmap, 0, 30, 0);
        // one slot in dst can not hold copied bits
        InnerTestForCopySlice(mBitmap, 0, 30, 20);
        InnerTestForCopySlice(mBitmap, 64, 30, 20);
        // srcIndex % 32 != 0
        // 1. leftBitsInDst >= leftBitsInSrc
        InnerTestForCopySlice(mBitmap, 5, 128, 1);
        InnerTestForCopySlice(mBitmap, 5, 128, 5);
        // 2. leftBitsInSrc > leftBitsInDst
        InnerTestForCopySlice(mBitmap, 5, 128, 28);
        InnerTestForCopySlice(mBitmap, 5, 128, 42);
        MyCleanUp();
    }

    void InnerTestForCopySlice2(int i, int j) 
    {
        Bitmap bitmap1((uint32_t)256);
        Bitmap bitmap2((uint32_t)256);
        for (int k = 0; k < 128; ++k) 
        {
            bitmap2.Set(rand() % 256);
        }
        Bitmap bitmap3 = bitmap1;
        Bitmap bitmap4 = bitmap2;
        bitmap1.CopySlice(i, bitmap2.GetData(), j, 77);
        for (int k = 0; k < 256; ++k) 
        {
            if (k < i) 
            {
                INDEXLIB_TEST_TRUE(bitmap3.Test(k) == bitmap1.Test(k));
            } 
            else if (k >= i + 77) 
            {
                INDEXLIB_TEST_TRUE(bitmap3.Test(k) == bitmap1.Test(k));
            } 
            else 
            {
                INDEXLIB_TEST_TRUE(bitmap4.Test(k - i + j) == bitmap1.Test(k));
            }
        }
    }

    void InnerTestForCopySlice(Bitmap *srcBitmap, uint32_t srcStartIndex, 
                               uint32_t copyItemCount, uint32_t dstStartIndex)
    {
        uint32_t itemCount = srcBitmap->GetItemCount();
        assert(srcStartIndex + copyItemCount < itemCount);
        Bitmap bitmap(itemCount);
        bitmap.CopySlice(dstStartIndex, srcBitmap->GetData(), 
                         srcStartIndex, copyItemCount);
        for (uint32_t i = 0; i < copyItemCount; i++)
        {
            INDEXLIB_TEST_EQUAL(srcBitmap->Test(srcStartIndex + i), 
                    bitmap.Test(dstStartIndex + i));
        }
        if (srcStartIndex % 2 == 0)
        {
            INDEXLIB_TEST_EQUAL((copyItemCount + 1) / 2 , 
                    bitmap.GetSetCount());
        }
        else
        {
            INDEXLIB_TEST_EQUAL(copyItemCount / 2 , bitmap.GetSetCount());
        }
    }

    void TestCaseForEmptyBitmap()
    {
        Bitmap bitmap((uint32_t)0);
        INDEXLIB_TEST_EQUAL((uint32_t)0, bitmap.GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)0, bitmap.GetSetCount());

        Bitmap *bitmap2 = bitmap.Clone();
        INDEXLIB_TEST_EQUAL((uint32_t)0, bitmap2->GetItemCount());
        INDEXLIB_TEST_EQUAL((uint32_t)0, bitmap2->GetSetCount());
        delete bitmap2;
    }

};

INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForAllocAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForAllocAllSet);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForSetAndTest);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForResetAll);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForResetAllMore);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForResetAllAfterHead);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForResetAllAfterMiddle);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForResetAllAfterTail);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForGetItemCount);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForSize);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForSizeOnBitMore);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForGetSetCount);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForMountAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForMountPartSet);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorSquareBracket);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorAndTowAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorAnd);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorAndEqualTowAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorAndEqual);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorOrTowAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorOr);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorOrEqualTowAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorOrEqual);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorNotAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorNot);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorMinusAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorMinus);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorMinusEqualAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForOperatorMinusEqual);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForEvaluateAllReset);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForEvaluate);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForReAlloc);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForGetSetCountWithRange);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForHasSetData);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForResetBetween);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForCopy);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForCopySlice);
INDEXLIB_UNIT_TEST_CASE(BitmapTest, TestCaseForEmptyBitmap);

IE_NAMESPACE_END(util);
