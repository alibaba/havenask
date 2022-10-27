#ifndef __INDEXLIB_MOUNTTABLETEST_H
#define __INDEXLIB_MOUNTTABLETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/mount_table.h"

IE_NAMESPACE_BEGIN(file_system);

class MountTableTest : public INDEXLIB_TESTBASE
{
public:
    MountTableTest();
    ~MountTableTest();

    DECLARE_CLASS_NAME(MountTableTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestSimpleProcess();
    void TestMount();
    void TestUnMount(); 
    void TestUnMountFail();
    void TestGetStorage();
    void TestGetStorageWithType();
    void TestGetStorageType();
    void TestGetFirstMatchPath();

    void TestMountException();

private:
    MountTablePtr CreateMountTable(const std::string& rootPath);
    void CheckMountTable(const MountTablePtr& mountTable, 
                         const std::string& expectPaths, int lineNo);
    void CheckUnMount(const std::string& mountTableStr,
                      const std::string& unMountPath,
                      const std::string& expectMountTableStr,
                      int lineNo);
    void CheckMatchPath(const MountTable& mountTable, std::string path,
                        bool expectMatch, std::string expectMatchPath,
                        FSStorageType expectType, int lineNo);

private:
    util::BlockMemoryQuotaControllerPtr mMemoryController;    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MountTableTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MountTableTest, TestMount);
INDEXLIB_UNIT_TEST_CASE(MountTableTest, TestUnMount);
INDEXLIB_UNIT_TEST_CASE(MountTableTest, TestUnMountFail);
INDEXLIB_UNIT_TEST_CASE(MountTableTest, TestGetStorage);
INDEXLIB_UNIT_TEST_CASE(MountTableTest, TestGetStorageWithType);
INDEXLIB_UNIT_TEST_CASE(MountTableTest, TestGetStorageType);
INDEXLIB_UNIT_TEST_CASE(MountTableTest, TestGetFirstMatchPath);

INDEXLIB_UNIT_TEST_CASE(MountTableTest, TestMountException);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MOUNTTABLETEST_H
