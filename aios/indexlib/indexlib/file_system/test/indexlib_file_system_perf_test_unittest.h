#ifndef __INDEXLIB_INDEXLIBFILESYSTEMPERFTEST_H
#define __INDEXLIB_INDEXLIBFILESYSTEMPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"

IE_NAMESPACE_BEGIN(file_system);

class IndexlibFileSystemPerfTest : public INDEXLIB_TESTBASE
{
public:
    IndexlibFileSystemPerfTest();
    ~IndexlibFileSystemPerfTest();

    DECLARE_CLASS_NAME(IndexlibFileSystemPerfTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void CheckOpenOtherFile();
    void CheckOpenSameFile();

private:
    std::string mReadFilePath;
    std::string mOtherFile;
    IndexlibFileSystemImplPtr mFileSystem;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexlibFileSystemPerfTest, TestSimpleProcess);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_INDEXLIBFILESYSTEMPERFTEST_H
