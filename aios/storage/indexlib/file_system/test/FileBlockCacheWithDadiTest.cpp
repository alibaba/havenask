#include "gtest/gtest.h"
#include <assert.h>
#include <cstddef>
#include <memory>

#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

using namespace indexlib::util;
namespace indexlib { namespace file_system {

class FileBlockCacheWithDadiTest : public INDEXLIB_TESTBASE
{
public:
    FileBlockCacheWithDadiTest();
    ~FileBlockCacheWithDadiTest();

    DECLARE_CLASS_NAME(FileBlockCacheWithDadiTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInitWithConfigStr();
    void TestMemoryQuotaController();

private:
    bool CheckInitWithConfigStr(const std::string& paramStr, int64_t expectCacheSize, int64_t expectDiskSize);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, FileBlockCacheWithDadiTest);

INDEXLIB_UNIT_TEST_CASE(FileBlockCacheWithDadiTest, TestInitWithConfigStr);
// INDEXLIB_UNIT_TEST_CASE(FileBlockCacheWithDadiTest, TestMemoryQuotaController);
// TODO (xiaohao.yxh) open this when mock sdk is ready

//////////////////////////////////////////////////////////////////////

FileBlockCacheWithDadiTest::FileBlockCacheWithDadiTest() {}

FileBlockCacheWithDadiTest::~FileBlockCacheWithDadiTest() {}

void FileBlockCacheWithDadiTest::CaseSetUp() {}

void FileBlockCacheWithDadiTest::CaseTearDown() {}

bool FileBlockCacheWithDadiTest::CheckInitWithConfigStr(const string& paramStr, int64_t expectMemorySizeInMb = -1,
                                                        int64_t expectDiskSizeInGb = -1)
{
    FileBlockCache fileBlockCache;
    util::MemoryQuotaControllerPtr memoryQuotaController =
        util::MemoryQuotaControllerCreator::CreateMemoryQuotaController();
    if (!fileBlockCache.Init(paramStr, memoryQuotaController)) {
        AUTIL_LOG(INFO, "init failed [%s]", paramStr.c_str());
        return false;
    }
    auto resourceInfo = fileBlockCache.GetResourceInfo();
    if (expectMemorySizeInMb >= 0 && (size_t)expectMemorySizeInMb * 1024 * 1024 != resourceInfo.maxMemoryUse) {
        AUTIL_LOG(INFO, "memory not equal [%lu] vs [%lu]", expectMemorySizeInMb * 1024 * 1024,
                  resourceInfo.maxMemoryUse);
        return false;
    }
    if (expectDiskSizeInGb >= 0 && (size_t)expectDiskSizeInGb * 1024 * 1024 * 1024 != resourceInfo.maxDiskUse) {
        AUTIL_LOG(INFO, "disk not equal [%lu] vs [%lu]", expectDiskSizeInGb * 1024 * 1024, resourceInfo.maxDiskUse);
        return false;
    }
    string cacheName(fileBlockCache.GetBlockCache()->TEST_GetCacheName());
    return cacheName == "dadiCache";
}

void FileBlockCacheWithDadiTest::TestInitWithConfigStr()
{
    // TODO(xiaohao.yxh) open valid check with mock sdk
    /*
        {
            // valid
            ASSERT_TRUE(CheckInitWithConfigStr("cache_type=dadi;memory_size_in_mb=2048;block_size=4096;disk_size_in_gb=16;",
                                               2048, 16));
            ASSERT_TRUE(CheckInitWithConfigStr("cache_type=dadi;memory_size_in_mb=2048;block_size=4096;disk_size_in_gb=17;",
                                               2048, 17));
            ASSERT_TRUE(CheckInitWithConfigStr(
                "cache_type=dadi;memory_size_in_mb=2048;block_size=4096;mapping_stack_type=1;disk_size_in_gb=2", 2048,
                2)); // mapping_stack_type=1 random mode  disk less than 16G
        }
    */

    {
        // invalid str
        ASSERT_FALSE(CheckInitWithConfigStr("cache_type=dadi;memory_size_in_mb"));
        ASSERT_FALSE(CheckInitWithConfigStr("cache_type=dadi;memory_size_in_mb=2048=1"));
        ASSERT_FALSE(CheckInitWithConfigStr("cache_type=dadi;memory_size_in_mb=-1"));
        ASSERT_FALSE(CheckInitWithConfigStr("cache_type=dadi;block_size=a"));
        ASSERT_FALSE(CheckInitWithConfigStr("cache_type=dadi;memory_size_in_mb=-1;block_size=10"));
    }

    {
        // invalid param
        ASSERT_FALSE(CheckInitWithConfigStr(
            "cache_type=dadi;memory_size_in_mb=1024;block_size=4096;disk_size_in_gb=16;")); // dadi memory size at least
                                                                                            // 2G
        ASSERT_FALSE(CheckInitWithConfigStr(
            "cache_type=dadi;memory_size_in_mb=2048;block_size=8193;disk_size_in_gb=16;")); // block size is multile of
                                                                                            // 4k
        ASSERT_FALSE(
            CheckInitWithConfigStr("cache_type=dadi;block_size=4096;disk_size_in_gb=16;")); // lack of memory_size_in_mb
        ASSERT_FALSE(CheckInitWithConfigStr("mapping_stack_type=0;cache_type=dadi;memory_size_in_mb=2048;block_size="
                                            "4096;disk_size_in_gb=1;")); // append mode at least 16G
                                                                         // disk
    }
}

void FileBlockCacheWithDadiTest::TestMemoryQuotaController()
{
    FileBlockCache fileBlockCache;
    util::MemoryQuotaControllerPtr memoryQuotaController =
        util::MemoryQuotaControllerCreator::CreateMemoryQuotaController(3UL * 1024 * 1024 * 1024);
    ASSERT_TRUE(fileBlockCache.Init("cache_type=dadi;memory_size_in_mb=2048;block_size=4096;disk_size_in_gb=16;",
                                    memoryQuotaController));
    ASSERT_EQ(2UL * 1024 * 1024 * 1024, fileBlockCache.GetResourceInfo().maxMemoryUse);
    ASSERT_EQ(1UL * 1024 * 1024 * 1024, memoryQuotaController->GetFreeQuota());
}
}} // namespace indexlib::file_system
