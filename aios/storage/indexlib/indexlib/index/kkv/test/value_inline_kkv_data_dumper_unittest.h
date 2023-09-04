#ifndef __INDEXLIB_VALUEINLINEKKVDATADUMPERTEST_H
#define __INDEXLIB_VALUEINLINEKKVDATADUMPERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/value_inline_kkv_data_dumper.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class ValueInlineKKVDataDumperTest : public INDEXLIB_TESTBASE
{
public:
    ValueInlineKKVDataDumperTest();
    ~ValueInlineKKVDataDumperTest();

    DECLARE_CLASS_NAME(ValueInlineKKVDataDumperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void InnerTestSimpleProcess(const std::string& skeyNodeInfoStr, const std::string& resultStrBeforeClose,
                                const std::string& resultStrAfterClose);

private:
    void CheckIter(const DumpablePKeyOffsetIteratorPtr& pkeyIter, const std::string& resultStr, regionid_t regionId);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ValueInlineKKVDataDumperTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_VALUEINLINEKKVDATADUMPERTEST_H
