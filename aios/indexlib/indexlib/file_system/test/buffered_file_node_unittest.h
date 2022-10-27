#ifndef __INDEXLIB_BUFFEREDFILENODETEST_H
#define __INDEXLIB_BUFFEREDFILENODETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/buffered_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class BufferedFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    BufferedFileNodeTest();
    ~BufferedFileNodeTest();

    DECLARE_CLASS_NAME(BufferedFileNodeTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNormalCase();
    void TestFileNotExist();
    void TestBufferedFileReaderNotSupport();

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};
INDEXLIB_UNIT_TEST_CASE(BufferedFileNodeTest, TestNormalCase);
INDEXLIB_UNIT_TEST_CASE(BufferedFileNodeTest, TestFileNotExist);
INDEXLIB_UNIT_TEST_CASE(BufferedFileNodeTest, TestBufferedFileReaderNotSupport);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BUFFEREDFILENODETEST_H
