#pragma once

#include "autil/Log.h"
#include "indexlib/config/customized_index_config.h"
#include "indexlib/util/testutil/unittest.h"

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
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CustomizedIndexConfigTest, TestSimpleProcess);
}} // namespace indexlib::config
