#ifndef __INDEXLIB_PATHMETACONTAINERTEST_H
#define __INDEXLIB_PATHMETACONTAINERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/path_meta_container.h"

IE_NAMESPACE_BEGIN(file_system);

class PathMetaContainerTest : public INDEXLIB_TESTBASE
{
public:
    PathMetaContainerTest();
    ~PathMetaContainerTest();

    DECLARE_CLASS_NAME(PathMetaContainerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestRemoveDirectory();

private:
    // path:len:time;path:len:time;
    std::vector<FileInfo> MakeFileInfo(const std::string& fileInfoStr);
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PathMetaContainerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PathMetaContainerTest, TestRemoveDirectory);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PATHMETACONTAINERTEST_H
