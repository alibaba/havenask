#include "indexlib/file_system/FileBlockCacheContainer.h"

#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class FileBlockCacheContainerTest : public INDEXLIB_TESTBASE
{
public:
    FileBlockCacheContainerTest();
    ~FileBlockCacheContainerTest();

    DECLARE_CLASS_NAME(FileBlockCacheContainerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestMultiBlockCache();
    void TestDuplicateLifeCycleBlockCache();
    void TestCreateTaskScheduler();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileBlockCacheContainerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(FileBlockCacheContainerTest, TestMultiBlockCache);
INDEXLIB_UNIT_TEST_CASE(FileBlockCacheContainerTest, TestDuplicateLifeCycleBlockCache);
INDEXLIB_UNIT_TEST_CASE(FileBlockCacheContainerTest, TestCreateTaskScheduler);

AUTIL_LOG_SETUP(indexlib.file_system, FileBlockCacheContainerTest);

FileBlockCacheContainerTest::FileBlockCacheContainerTest() {}

FileBlockCacheContainerTest::~FileBlockCacheContainerTest() {}

void FileBlockCacheContainerTest::CaseSetUp() {}

void FileBlockCacheContainerTest::CaseTearDown() {}

void FileBlockCacheContainerTest::TestSimpleProcess()
{
    MemoryQuotaControllerPtr memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(10 * 1024 * 1024);

    {
        FileBlockCacheContainerPtr container(new FileBlockCacheContainer());
        ASSERT_TRUE(container->Init("cache_size=10", memoryQuotaController));
        auto cache = container->GetAvailableFileCache("");
        ASSERT_TRUE(cache);
        ASSERT_EQ(10 * 1024 * 1024, cache->GetResourceInfo().maxMemoryUse);
    }

    {
        FileBlockCacheContainerPtr container(new FileBlockCacheContainer());
        // use default cache size = 1
        ASSERT_TRUE(container->Init("", memoryQuotaController));
        auto cache = container->GetAvailableFileCache("");
        ASSERT_TRUE(cache);
        ASSERT_EQ(1 * 1024 * 1024, cache->GetResourceInfo().maxMemoryUse);
    }
}

void FileBlockCacheContainerTest::TestMultiBlockCache()
{
    MemoryQuotaControllerPtr memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(30 * 1024 * 1024);

    FileBlockCacheContainerPtr container(new FileBlockCacheContainer());
    ASSERT_TRUE(container->Init("cache_size=10;life_cycle=hot|cache_size=15;life_cycle=cold", memoryQuotaController));
    {
        auto cache = container->GetAvailableFileCache("");
        ASSERT_TRUE(cache);
        ASSERT_EQ(1 * 1024 * 1024, cache->GetResourceInfo().maxMemoryUse);
    }

    {
        auto cache = container->GetAvailableFileCache("hot");
        ASSERT_TRUE(cache);
        ASSERT_EQ(10 * 1024 * 1024, cache->GetResourceInfo().maxMemoryUse);
    }

    {
        auto cache = container->GetAvailableFileCache("cold");
        ASSERT_TRUE(cache);
        ASSERT_EQ(15 * 1024 * 1024, cache->GetResourceInfo().maxMemoryUse);
    }
}

void FileBlockCacheContainerTest::TestDuplicateLifeCycleBlockCache()
{
    MemoryQuotaControllerPtr memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(30 * 1024 * 1024);

    FileBlockCacheContainerPtr container(new FileBlockCacheContainer());
    ASSERT_FALSE(container->Init("cache_size=10;life_cycle=hot|cache_size=15;life_cycle=hot", memoryQuotaController));
}

void FileBlockCacheContainerTest::TestCreateTaskScheduler()
{
    MemoryQuotaControllerPtr memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(30 * 1024 * 1024);

    FileBlockCacheContainerPtr container(new FileBlockCacheContainer());
    container->Init("cache_size=10;life_cycle=hot", memoryQuotaController, util::TaskSchedulerPtr(),
                    util::MetricProviderPtr(new util::MetricProvider()));
    ASSERT_TRUE(container->_taskScheduler);
    ASSERT_TRUE(container->_taskScheduler->GetTaskCount() > 0);
}

}} // namespace indexlib::file_system
