#ifndef __INDEXLIB_MERGEIOCONFIGTEST_H
#define __INDEXLIB_MERGEIOCONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/merge_io_config.h"

IE_NAMESPACE_BEGIN(config);

class MergeIOConfigTest : public INDEXLIB_TESTBASE
{
public:
    MergeIOConfigTest();
    ~MergeIOConfigTest();

    DECLARE_CLASS_NAME(MergeIOConfigTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestJsonize();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeIOConfigTest, TestJsonize);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_MERGEIOCONFIGTEST_H
