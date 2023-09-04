#include "indexlib/util/test/resource_control_thread_pool_unittest.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace util {
IE_LOG_SETUP(util, ResourceControlThreadPoolTest);

namespace {
class MockResourceControlWorkItem : public ResourceControlWorkItem
{
public:
    MockResourceControlWorkItem(int64_t identifier, int64_t requiredResource)
        : mIdentifier(identifier)
        , mRequiredResource(requiredResource)
    {
    }

public:
    int64_t GetRequiredResource() const { return mRequiredResource; }
    void Process()
    {
        {
            ScopedLock lock(mMutex);
            mIdentiVec.push_back(mIdentifier);
        }
        // 200 ms
        usleep(200000);
    }
    void Destroy() {}

public:
    static vector<int64_t> mIdentiVec;

private:
    static ThreadMutex mMutex;
    int64_t mIdentifier;
    int64_t mRequiredResource;
};

ThreadMutex MockResourceControlWorkItem::mMutex;
vector<int64_t> MockResourceControlWorkItem::mIdentiVec;

class MockWorkItemWithException : public ResourceControlWorkItem
{
public:
    MOCK_METHOD(int64_t, GetRequiredResource, (), (const, override));
    MOCK_METHOD(void, Destroy, (), (override));
    MOCK_METHOD(void, Drop, (), ());
    MOCK_METHOD(void, Process, (), (override));
};
} // namespace

ResourceControlThreadPoolTest::ResourceControlThreadPoolTest() {}

ResourceControlThreadPoolTest::~ResourceControlThreadPoolTest() {}

void ResourceControlThreadPoolTest::CaseSetUp() { MockResourceControlWorkItem::mIdentiVec.clear(); }

void ResourceControlThreadPoolTest::CaseTearDown() {}

void ResourceControlThreadPoolTest::TestSimpleProcess()
{
    ResourceControlThreadPool pool(2, 100);
    vector<ResourceControlWorkItemPtr> workItems;
    workItems.push_back(ResourceControlWorkItemPtr(new MockResourceControlWorkItem(1, 70)));
    workItems.push_back(ResourceControlWorkItemPtr(new MockResourceControlWorkItem(2, 80)));
    workItems.push_back(ResourceControlWorkItemPtr(new MockResourceControlWorkItem(3, 30)));
    pool.Init(workItems);
    pool.Run("");
    ASSERT_EQ(string("1 3 2"), StringUtil::toString(MockResourceControlWorkItem::mIdentiVec));
}

void ResourceControlThreadPoolTest::TestResourceNotEnough()
{
    ResourceControlThreadPool pool(2, 100);
    vector<ResourceControlWorkItemPtr> workItems;
    workItems.push_back(ResourceControlWorkItemPtr(new MockResourceControlWorkItem(1, 70)));
    workItems.push_back(ResourceControlWorkItemPtr(new MockResourceControlWorkItem(2, 60)));
    workItems.push_back(ResourceControlWorkItemPtr(new MockResourceControlWorkItem(3, 50)));

    pool.Init(workItems);
    pool.Run("");
    ASSERT_EQ(string("1 2 3"), StringUtil::toString(MockResourceControlWorkItem::mIdentiVec));
}

void ResourceControlThreadPoolTest::TestInit()
{
    ResourceControlThreadPool pool(2, 50);
    vector<ResourceControlWorkItemPtr> workItems;
    workItems.push_back(ResourceControlWorkItemPtr(new MockResourceControlWorkItem(1, 70)));
    ASSERT_THROW(pool.Init(workItems), OutOfRangeException);
}

void ResourceControlThreadPoolTest::TestThrowException()
{
    {
        // test require resource exception
        ResourceControlThreadPool pool(2, 100);
        vector<ResourceControlWorkItemPtr> workItems;
        MockWorkItemWithException* mockWorkItem = new MockWorkItemWithException();
        workItems.push_back(ResourceControlWorkItemPtr(mockWorkItem));
        EXPECT_CALL(*mockWorkItem, GetRequiredResource()).WillOnce(Throw(FileIOException()));
        ASSERT_THROW(pool.Init(workItems), FileIOException);
    }

    {
        // test pop require resource exception
        ResourceControlThreadPool pool(2, 100);
        vector<ResourceControlWorkItemPtr> workItems;
        MockWorkItemWithException* mockWorkItem = new MockWorkItemWithException();
        workItems.push_back(ResourceControlWorkItemPtr(mockWorkItem));
        EXPECT_CALL(*mockWorkItem, GetRequiredResource()).WillOnce(Return(50)).WillRepeatedly(Throw(FileIOException()));
        EXPECT_CALL(*mockWorkItem, Destroy()).WillOnce(Return());
        ASSERT_NO_THROW(pool.Init(workItems));
        ASSERT_THROW(pool.Run(""), ExceptionBase);
    }

    {
        // test process exception
        ResourceControlThreadPool pool(2, 100);
        vector<ResourceControlWorkItemPtr> workItems;
        MockWorkItemWithException* mockWorkItem = new MockWorkItemWithException();
        workItems.push_back(ResourceControlWorkItemPtr(mockWorkItem));
        EXPECT_CALL(*mockWorkItem, GetRequiredResource()).WillRepeatedly(Return(50));
        EXPECT_CALL(*mockWorkItem, Process()).WillRepeatedly(Throw(FileIOException()));
        EXPECT_CALL(*mockWorkItem, Destroy()).WillOnce(Return());
        ASSERT_NO_THROW(pool.Init(workItems));
        ASSERT_THROW(pool.Run(""), ExceptionBase);
    }

    {
        // test destroy exception
        ResourceControlThreadPool pool(2, 100);
        vector<ResourceControlWorkItemPtr> workItems;
        MockWorkItemWithException* mockWorkItem = new MockWorkItemWithException();
        workItems.push_back(ResourceControlWorkItemPtr(mockWorkItem));
        EXPECT_CALL(*mockWorkItem, GetRequiredResource()).WillRepeatedly(Return(50));
        EXPECT_CALL(*mockWorkItem, Process()).WillOnce(Return());
        EXPECT_CALL(*mockWorkItem, Destroy()).WillRepeatedly(Throw(FileIOException()));
        ASSERT_NO_THROW(pool.Init(workItems));
        ASSERT_THROW(pool.Run(""), ExceptionBase);
    }
}
}} // namespace indexlib::util
