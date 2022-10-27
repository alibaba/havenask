#ifndef __INDEXLIB_DISKSTORAGETEST_H
#define __INDEXLIB_DISKSTORAGETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/load_config/load_config_list.h"
#include "indexlib/file_system/disk_storage.h"

IE_NAMESPACE_BEGIN(file_system);

class DiskStorageTest : public INDEXLIB_TESTBASE
{
public:
    DiskStorageTest();
    ~DiskStorageTest();

    DECLARE_CLASS_NAME(DiskStorageTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestLocalInit();
    void TestMmapFileReader();
    void TestBlockFileReader();
    void TestGlobalBlockFileReader();
    void TestInMemFileReader();
    void TestBufferedFileReader();
    void TestBufferedFileWriter();
    void TestUnknownFileReader();
    void TestNoLoadConfigListWithCacheHit();
    void TestNoLoadConfigListWithCacheMiss();
    void TestLoadConfigListWithNoCache();
    void TestLoadConfigListWithCacheHit();
    void TestLoadConfigListWithCacheMiss();
    void TestBlockFileLoadConfigMatch();
    void TestLoadWithLifecycleConfig();
    void TestStorageMetrics();
    void TestStorageMetricsForCacheMiss();
    void TestRemoveFile();
    void TestRemoveDirectory();
    void TestIsExist();

private:
    void CreateLoadConfigList(LoadConfigList& loadConfigList);
    void CheckFileReader(DiskStoragePtr storage, std::string filePath, FSOpenType type,
        std::string expectContext, bool fromLoadConfig = false, bool writable = false);

    DiskStoragePtr PrepareDiskStorage();
    void InnerTestBufferedFileWriter(bool atomicWrite);
    void InnerTestLifecycle(const std::string &loadConfigJsonStr,
                            const std::string& matchPath, const std::string lifecycle,
                            FSOpenType openType, bool expectLock,
                            std::function<bool(FileNodePtr &)> getLockModeFunc,
                            std::function<void(FileNodePtr &)> checkFunc);

private:
    std::string mRootDir;
    util::BlockMemoryQuotaControllerPtr mMemoryController;
    FileBlockCachePtr mFileBlockCache;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLocalInit);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestMmapFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestBlockFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestGlobalBlockFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestInMemFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestBufferedFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestBufferedFileWriter);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestUnknownFileReader);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestNoLoadConfigListWithCacheHit);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestNoLoadConfigListWithCacheMiss);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLoadConfigListWithNoCache);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLoadConfigListWithCacheHit);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLoadConfigListWithCacheMiss);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestBlockFileLoadConfigMatch);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestLoadWithLifecycleConfig);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestStorageMetrics);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestStorageMetricsForCacheMiss);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(DiskStorageTest, TestIsExist);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DISKSTORAGETEST_H
