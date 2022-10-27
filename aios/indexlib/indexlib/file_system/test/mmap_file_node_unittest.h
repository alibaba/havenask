#ifndef __INDEXLIB_MMAPFILENODETEST_H
#define __INDEXLIB_MMAPFILENODETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/load_config/load_config.h"
#include "indexlib/file_system/mmap_file_node.h"

IE_NAMESPACE_BEGIN(file_system);

class MmapFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    MmapFileNodeTest();
    ~MmapFileNodeTest();

    DECLARE_CLASS_NAME(MmapFileNodeTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestMmapFileNode();
    void TestCaseForOpen();
    void TestCaseForRead();
    void TestCaseForReadWrite();
    void TestCaseForReadEmptyFile();
    void TestCaseForLock();
    void TestCaseForWrite();
    void TestCaseForCloseBeforeOpen();

private:
    void MakeData(const std::string& filePath, std::vector<uint8_t>& answer);
    MmapFileNodePtr CreateFileNode(const std::string& filePath, FSOpenType type);
    void Check(FileNodePtr fileNode, uint8_t* answer, uint32_t stepLen);


private:
    std::string mRootDir;
    LoadConfig mLoadConfig;
    util::BlockMemoryQuotaControllerPtr mMemoryController;
    static const uint32_t TOTAL_BYTE = 1000;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestMmapFileNode);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForOpen);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForRead);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForReadWrite);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForReadEmptyFile);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForLock);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForWrite);
INDEXLIB_UNIT_TEST_CASE(MmapFileNodeTest, TestCaseForCloseBeforeOpen);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_MMAPFILENODETEST_H
