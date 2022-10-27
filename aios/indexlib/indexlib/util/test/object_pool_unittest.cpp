#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/object_pool.h"
#include <sstream>
#include <tr1/functional>

using namespace std;

IE_NAMESPACE_BEGIN(util);

class A
{
public:
    typedef ObjectPool<A> APool;
    
    A()
    {
    }
    A(APool* pool)
        : mOwner(pool)
    {
    }

    void Free()
    {
        mOwner->Free(this);
        mText = "";
    }

    void SetOwner(APool* pool)
    {
        mOwner = pool;
    }

    void SetMsg(const string& text)
    {
        mText = text;
    }
    
    const string& GetMsg() const
    {
        return mText;
    }

private:
    APool* mOwner;
    string mText;
};
typedef std::tr1::shared_ptr<A> APtr;

class ObjectPoolTest : public INDEXLIB_TESTBASE
{
public:
    typedef ObjectPool<A> APool;
    static const int POOL_SIZE = 100;
    DECLARE_CLASS_NAME(ObjectPoolTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForAPool()
    {
        int i;
        APool pool;
        pool.Init(POOL_SIZE);
        A* aList[POOL_SIZE];

        for (i = 0; i < POOL_SIZE; ++i)
        {
            aList[i] = pool.Alloc();
            ostringstream os;
            os << "text" << i;
            aList[i]->SetMsg(os.str());
        }

        for (i = POOL_SIZE / 2; i < POOL_SIZE; ++i)
        {
            pool.Free(aList[i]);
        }

        for (i = POOL_SIZE / 2; i < POOL_SIZE; ++i)
        {
            aList[i] = pool.Alloc();
            ostringstream os;
            os << "TEXT" << i;
            aList[i]->SetMsg(os.str());
        }

        for (i = 0; i < POOL_SIZE / 2; ++i)
        {
            ostringstream os;
            os << "text" << i;
            INDEXLIB_TEST_TRUE(os.str() == aList[i]->GetMsg());
        }

        for (i = POOL_SIZE / 2; i < POOL_SIZE; ++i)
        {
            ostringstream os;
            os << "TEXT" << i;
            INDEXLIB_TEST_TRUE(os.str() == aList[i]->GetMsg());
        }
    }

    void TestCaseForLazyAlloc()
    {
        APool pool;
        pool.Init(POOL_SIZE);
        INDEXLIB_TEST_TRUE(NULL == pool.mObjects);
        INDEXLIB_TEST_TRUE(NULL == pool.mFreeObjectIndexes);

        // A *a = NULL;
        // a = pool.Alloc();
        // INDEXLIB_TEST_TRUE(a);

        // INDEXLIB_TEST_TRUE(pool.mObjects);
        // INDEXLIB_TEST_TRUE(pool.mFreeObjectIndexes);
    }
    
    void TestCaseForSharedPtr()
    {
        APool pool;
        pool.Init(POOL_SIZE);
        {
            APtr a(pool.Alloc(), std::tr1::bind(&APool::Free, &pool,
                            std::tr1::placeholders::_1));
        }
    }

};

INDEXLIB_UNIT_TEST_CASE(ObjectPoolTest, TestCaseForAPool);
INDEXLIB_UNIT_TEST_CASE(ObjectPoolTest, TestCaseForLazyAlloc);
INDEXLIB_UNIT_TEST_CASE(ObjectPoolTest, TestCaseForSharedPtr);

IE_NAMESPACE_END(util);
