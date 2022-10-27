#ifndef __INDEXLIB_RAIDCONFIGTEST_H
#define __INDEXLIB_RAIDCONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/storage/raid_config.h"

IE_NAMESPACE_BEGIN(storage);

class RaidConfigTest : public INDEXLIB_TESTBASE
{
public:
    RaidConfigTest();
    ~RaidConfigTest();

    DECLARE_CLASS_NAME(RaidConfigTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RaidConfigTest, TestSimpleProcess);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_RAIDCONFIGTEST_H
