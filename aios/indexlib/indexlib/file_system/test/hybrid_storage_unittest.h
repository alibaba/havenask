#ifndef __INDEXLIB_HYBRIDSTORAGETEST_H
#define __INDEXLIB_HYBRIDSTORAGETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/hybrid_storage.h"
#include "indexlib/file_system/load_config/load_config_list.h"

IE_NAMESPACE_BEGIN(file_system);

class HybridStorageTest : public INDEXLIB_TESTBASE
{
public:
    HybridStorageTest();
    ~HybridStorageTest();

    DECLARE_CLASS_NAME(HybridStorageTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCreateFileWriter();
    void TestCreateFileReader();
    void TestDirectory();
    void TestIsExistAndListFile(); // IsExist, IsExistOnDisk, ListFile

    // TODO
    // PackageFile

    void TestFileInfo(); // GetFileLengh GetFileStat
    void TestDirInfo(); // IsDir

    void TestFileNodeCreator();
    void TestPackageFile();
    void TestPackageFileAndSharedMMap(); // dcache
    void TestPackageFileAndSharedMMapWithBufferedFile(); // dcache
    void TestPackageFileAndSharedBuffer(); // dcache
    void TestCleanSharePackageFileWithFileOpened();
    void TestSepratePackageFile();

    void TestMmapInDfs();
    void TestMmapInDCache();

    void TestUsePathMetaCache();

private:
    std::string GetPrimaryPath(const std::string& relativePath);
    std::string GetSecondaryPath(const std::string& relativePath);

    HybridStoragePtr PrepareHybridStorage(
        const std::string& primaryData, const std::string& secondaryData,
        bool isOffline = false);

    LoadConfigList PrepareData(const std::string& fileInfos);

    void InnerTestCreateFileReader(bool primary, bool secondary, bool withCache);
    void CheckReader(const StoragePtr& storage, const std::string& path,
                     FSOpenType openType, FSFileType fileType);

    void MakePackageFile(const std::string& dirName,
                         const std::string& packageFileInfoStr);
    FileReaderPtr CheckPackageFile(const StoragePtr& storage, const std::string& fileName,
                                   const std::string& content, FSOpenType type);
    void DoTestUsePathMetaCache(bool usePathMetaCache, bool isOffline);
    

private:
    std::string mPrimaryRoot;
    std::string mSecondaryRoot;
    FileBlockCachePtr mFileBlockCache;
    util::BlockMemoryQuotaControllerPtr mMemoryController;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestCreateFileWriter);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestCreateFileReader);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestDirectory);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestIsExistAndListFile);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestFileInfo);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestFileNodeCreator);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestPackageFile);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestPackageFileAndSharedMMap);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestPackageFileAndSharedMMapWithBufferedFile);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestCleanSharePackageFileWithFileOpened);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestSepratePackageFile);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestMmapInDfs);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestMmapInDCache);
INDEXLIB_UNIT_TEST_CASE(HybridStorageTest, TestUsePathMetaCache);


IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_HYBRIDSTORAGETEST_H
