#ifndef __INDEXLIB_BLOCKFPENCODERTEST_H
#define __INDEXLIB_BLOCKFPENCODERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/block_fp_encoder.h"

IE_NAMESPACE_BEGIN(util);

class BlockFpEncoderTest : public INDEXLIB_TESTBASE
{
public:
    BlockFpEncoderTest();
    ~BlockFpEncoderTest();

    DECLARE_CLASS_NAME(BlockFpEncoderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCompressFromFile();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BlockFpEncoderTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(BlockFpEncoderTest, TestCompressFromFile);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BLOCKFPENCODERTEST_H
