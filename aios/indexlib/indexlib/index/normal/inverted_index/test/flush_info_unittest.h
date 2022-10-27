#ifndef __INDEXLIB_FLUSHINFOTEST_H
#define __INDEXLIB_FLUSHINFOTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/format/flush_info.h"

IE_NAMESPACE_BEGIN(index);

class FlushInfoTest : public INDEXLIB_TESTBASE {
public:
    FlushInfoTest();
    ~FlushInfoTest();

    DECLARE_CLASS_NAME(FlushInfoTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FlushInfoTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FLUSHINFOTEST_H
