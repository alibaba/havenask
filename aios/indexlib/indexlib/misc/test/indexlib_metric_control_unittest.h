#ifndef __INDEXLIB_INDEXLIBMETRICCONTROLLTEST_H
#define __INDEXLIB_INDEXLIBMETRICCONTROLLTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/misc/indexlib_metric_control.h"

IE_NAMESPACE_BEGIN(misc);

class IndexlibMetricControlTest : public INDEXLIB_TESTBASE
{
public:
    IndexlibMetricControlTest();
    ~IndexlibMetricControlTest();

    DECLARE_CLASS_NAME(IndexlibMetricControlTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void Check();
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexlibMetricControlTest, TestSimpleProcess);

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_INDEXLIBMETRICCONTROL_H
