#ifndef __INDEXLIB_KKVPERFTEST_H
#define __INDEXLIB_KKVPERFTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/test/online_partition_perf_test.h"
#include "indexlib/test/multi_thread_test_base.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class KKVPerfTest : public OnlinePartitionPerfTest
{
public:
    KKVPerfTest();
    ~KKVPerfTest();

    DECLARE_CLASS_NAME(KKVPerfTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();

private:
    void DoRead(int* errCode) override;
    std::string MakeDocStr() override;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KKVPerfTest, TestSimpleProcess);
}} // namespace indexlib::partition

#endif //__INDEXLIB_KKVPERFTEST_H
