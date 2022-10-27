#ifndef __INDEXLIB_FLUSHEDLOCATORCONTAINERTEST_H
#define __INDEXLIB_FLUSHEDLOCATORCONTAINERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/flushed_locator_container.h"

IE_NAMESPACE_BEGIN(partition);

class FlushedLocatorContainerTest : public INDEXLIB_TESTBASE
{
public:
    FlushedLocatorContainerTest();
    ~FlushedLocatorContainerTest();

    DECLARE_CLASS_NAME(FlushedLocatorContainerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FlushedLocatorContainerTest, TestSimpleProcess);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FLUSHEDLOCATORCONTAINERTEST_H
