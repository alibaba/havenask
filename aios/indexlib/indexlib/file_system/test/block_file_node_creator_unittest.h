#ifndef __INDEXLIB_BLOCKFILENODECREATORTEST_H
#define __INDEXLIB_BLOCKFILENODECREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/block_file_node_creator.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"

IE_NAMESPACE_BEGIN(file_system);

class BlockFileNodeCreatorTest : public INDEXLIB_TESTBASE
{
public:
    BlockFileNodeCreatorTest();
    ~BlockFileNodeCreatorTest();

    DECLARE_CLASS_NAME(BlockFileNodeCreatorTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForInit();
    void TestCaseForBadInit();

private:
    std::string mRootDir;
    util::BlockMemoryQuotaControllerPtr mMemoryController;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BlockFileNodeCreatorTest, TestCaseForInit);
INDEXLIB_UNIT_TEST_CASE(BlockFileNodeCreatorTest, TestCaseForBadInit);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_BLOCKFILENODECREATORTEST_H
