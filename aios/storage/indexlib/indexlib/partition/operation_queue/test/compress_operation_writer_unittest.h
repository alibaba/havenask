#ifndef __INDEXLIB_COMPRESSOPERATIONWRITERTEST_H
#define __INDEXLIB_COMPRESSOPERATIONWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/compress_operation_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class CompressOperationWriterTest : public INDEXLIB_TESTBASE
{
public:
    CompressOperationWriterTest();
    ~CompressOperationWriterTest();

    DECLARE_CLASS_NAME(CompressOperationWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CompressOperationWriterTest, TestSimpleProcess);
}} // namespace indexlib::partition

#endif //__INDEXLIB_COMPRESSOPERATIONWRITERTEST_H
