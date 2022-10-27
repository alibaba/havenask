#include "indexlib/util/test/thread_pool_unittest.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ThreadPoolTest);
namespace
{
class MockWorkItem : public WorkItem
{
public:
    MOCK_METHOD0(process, void());
    MOCK_METHOD0(destroy, void());
    MOCK_METHOD0(drop, void());
};
}
ThreadPoolTest::ThreadPoolTest()
{
}

ThreadPoolTest::~ThreadPoolTest()
{
}

void ThreadPoolTest::CaseSetUp()
{
}

void ThreadPoolTest::CaseTearDown()
{
}

void ThreadPoolTest::TestThreadPoolExceptionStop()
{
    MockWorkItem item1;
    EXPECT_CALL(item1, process())
        .WillOnce(Throw(FileIOException()));
    EXPECT_CALL(item1, destroy())
        .WillOnce(Throw(FileIOException()));
    MockWorkItem item2;
    EXPECT_CALL(item2, drop())
        .WillOnce(Return());
    ThreadPool pool(1, 100);
    pool.PushWorkItem(&item1);
    pool.PushWorkItem(&item2);
    pool.Start("");
    ASSERT_THROW(pool.Stop(), ExceptionBase);
}

void ThreadPoolTest::TestThreadPoolWithException()
{
    {
        //test stop exception
        MockWorkItem item1;
        EXPECT_CALL(item1, process())
            .WillOnce(Throw(FileIOException()));
        EXPECT_CALL(item1, destroy())
            .WillOnce(Throw(FileIOException()));
        
        ThreadPool pool(2, 100);
        pool.Start("");
        ASSERT_NO_THROW(pool.PushWorkItem(&item1));
        ASSERT_THROW(pool.Stop(), ExceptionBase);
    }

    {
        //test push back exception
        MockWorkItem item1;
        EXPECT_CALL(item1, process())
            .WillOnce(Throw(FileIOException()));
        EXPECT_CALL(item1, destroy())
            .WillOnce(Throw(FileIOException()));

        ThreadPool pool(2, 100);
        pool.Start("");
        ASSERT_NO_THROW(pool.PushWorkItem(&item1));
        MockWorkItem item2;
        EXPECT_CALL(item2, drop())
            .WillOnce(Return());
        usleep(200000);
        ASSERT_THROW(pool.PushWorkItem(&item2), ExceptionBase);
    }

    {
        //test destroy exception
        MockWorkItem item1;
        EXPECT_CALL(item1, process())
            .WillOnce(Return());
        EXPECT_CALL(item1, destroy())
            .WillOnce(Throw(FileIOException()))
            .WillOnce(Throw(FileIOException()));

        ThreadPool pool(2, 100);
        pool.Start("");
        ASSERT_NO_THROW(pool.PushWorkItem(&item1));
        ASSERT_THROW(pool.Stop(), ExceptionBase);
    }
}

IE_NAMESPACE_END(util);

