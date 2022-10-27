#ifndef __INDEXLIB_LOADCONFIGLISTTEST_H
#define __INDEXLIB_LOADCONFIGLISTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/indexlib_file_system.h"

IE_NAMESPACE_BEGIN(file_system);

class LoadConfigListTest : public INDEXLIB_TESTBASE
{
public:
    LoadConfigListTest();
    ~LoadConfigListTest();

    DECLARE_CLASS_NAME(LoadConfigListTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestLoadConfigListWithMacro();
    void TestGetLoadConfigOpenType();

private:
    LoadConfigList CreateLoadConfigWithMacro();
    void CheckFileReader(const IndexlibFileSystemPtr& ifs, 
                         const std::string& filePath, FSOpenType openType,
                         FSFileType fileType, int lineNo);
    void DoGetLoadConfigOpenType(std::string strategyName,
                                 FSOpenType expectOpenType);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestLoadConfigListWithMacro);
INDEXLIB_UNIT_TEST_CASE(LoadConfigListTest, TestGetLoadConfigOpenType);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LOADCONFIGLISTTEST_H
