#ifndef __INDEXLIB_FILENODECACHETEST_H
#define __INDEXLIB_FILENODECACHETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/file_node_cache.h"

IE_NAMESPACE_BEGIN(file_system);

class FileNodeCacheTest : public INDEXLIB_TESTBASE
{
public:
    FileNodeCacheTest();
    ~FileNodeCacheTest();

    DECLARE_CLASS_NAME(FileNodeCacheTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNormal();
    void TestListFile();
    void TestRemoveFile();
    void TestRemoveDirectory();
    void TestClean();
    void TestCleanFiles();
    void TestGetUseCount();

private:
    FileNodePtr CreateFileNode(const std::string& path = "",
                               FSFileType type = FSFT_IN_MEM,
                               bool isDirty = false);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestNormal);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestListFile);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestClean);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestCleanFiles);
INDEXLIB_UNIT_TEST_CASE(FileNodeCacheTest, TestGetUseCount);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILENODECACHETEST_H
