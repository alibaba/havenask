#ifndef __INDEXLIB_DIRECTORYTEST_H
#define __INDEXLIB_DIRECTORYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/load_config/load_strategy.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(file_system);

class DirectoryTest : public INDEXLIB_TESTBASE
{
public:
    DirectoryTest();
    ~DirectoryTest();

    DECLARE_CLASS_NAME(DirectoryTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestStoreAndLoad();
    void TestCreateIntegratedFileReader();
    void TestCreateIntegratedFileReaderNoReplace();
    void TestCreateCompressFileReader();
    void TestProhibitInMemDump();
    void TestPatialLock();
    void TestAddSolidPathFileInfos();
    void TestRename();
    void TestGetArchiveFolder();

private:
    enum CompressReaderType
    {
        COMP_READER_INTE,
        COMP_READER_NORMAL,
        COMP_READER_CACHE,
    };
    
private:
    void DoTestCreateIntegratedFileReader(std::string loadType, 
            FSOpenType expectOpenType);

    config::IndexPartitionOptions CreateOptionWithLoadConfig(
            std::string loadType);
    config::IndexPartitionOptions CreateOptionWithLoadConfig(
        const std::vector<std::pair<std::string, LoadStrategy*>>& configs);

    void MakeCompressFile(const std::string& fileName, size_t bufferSize);

    void CheckReaderType(const DirectoryPtr& dir, const std::string& fileName,
                         CompressReaderType readerType);
    
private:
    DirectoryPtr mDirectory;
    std::string mRootDir;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestStoreAndLoad);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestCreateIntegratedFileReader);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestCreateIntegratedFileReaderNoReplace);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestCreateCompressFileReader);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestProhibitInMemDump);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestPatialLock);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestAddSolidPathFileInfos);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestRename);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestGetArchiveFolder);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DIRECTORYTEST_H
