#ifndef __INDEXLIB_CUSTOMIZEDINDEXCONFIGTEST_H
#define __INDEXLIB_CUSTOMIZEDINDEXCONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

class CustomizedIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    CustomizedIndexConfigTest();
    ~CustomizedIndexConfigTest();

    DECLARE_CLASS_NAME(CustomizedIndexConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomizedIndexConfigTest, TestSimpleProcess);
}} // namespace indexlib::config

#endif //__INDEXLIB_CUSTOMIZEDINDEXCONFIGTEST_H
