#ifndef __INDEXLIB_SUMMARYREADERINTETEST_H
#define __INDEXLIB_SUMMARYREADERINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(index);

class SummaryReaderIntetest : public INDEXLIB_TESTBASE
{
public:
    SummaryReaderIntetest();
    ~SummaryReaderIntetest();

    DECLARE_CLASS_NAME(SummaryReaderIntetest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGroup();
    void TestAllGroupNoNeedStore();
    void TestDefaultGroupNoNeedStore();
    void TestCompress();
    void TestCompressMultiThread();

private:
    void CheckSummary(docid_t docId,
                      const std::vector<summarygroupid_t> groupVec,
                      const std::string& expectStr = "");
private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    std::string mJsonStringHead;
    std::string mJsonStringTail;
    config::IndexPartitionSchemaPtr mSchema;
    test::PartitionStateMachine mPsm;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SummaryReaderIntetest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SummaryReaderIntetest, TestGroup);
INDEXLIB_UNIT_TEST_CASE(SummaryReaderIntetest, TestAllGroupNoNeedStore);
INDEXLIB_UNIT_TEST_CASE(SummaryReaderIntetest, TestDefaultGroupNoNeedStore);
INDEXLIB_UNIT_TEST_CASE(SummaryReaderIntetest, TestCompress);
INDEXLIB_UNIT_TEST_CASE(SummaryReaderIntetest, TestCompressMultiThread);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUMMARYREADERINTETEST_H
