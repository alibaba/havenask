#ifndef __INDEXLIB_FILENODECREATORTEST_H
#define __INDEXLIB_FILENODECREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/file_node_creator.h"

IE_NAMESPACE_BEGIN(file_system);

class FileNodeCreatorTest : public INDEXLIB_TESTBASE
{
public:
    FileNodeCreatorTest();
    ~FileNodeCreatorTest();

    DECLARE_CLASS_NAME(FileNodeCreatorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOpenFileNode();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileNodeCreatorTest, TestOpenFileNode);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILENODECREATORTEST_H
