#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

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
}} // namespace indexlib::partition
