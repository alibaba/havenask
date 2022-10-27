#ifndef __INDEXLIB_PACKDIRECTORYTEST_H
#define __INDEXLIB_PACKDIRECTORYTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/pack_directory.h"

IE_NAMESPACE_BEGIN(file_system);

class PackDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    PackDirectoryTest();
    ~PackDirectoryTest();

    DECLARE_CLASS_NAME(PackDirectoryTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
    void TestCreateFileWriter();
    void TestMakeDirectory();
    void TestCreateInMemoryFile();
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackDirectoryTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(PackDirectoryTest, TestCreateFileWriter);
INDEXLIB_UNIT_TEST_CASE(PackDirectoryTest, TestMakeDirectory);
INDEXLIB_UNIT_TEST_CASE(PackDirectoryTest, TestCreateInMemoryFile);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_PACKDIRECTORYTEST_H
