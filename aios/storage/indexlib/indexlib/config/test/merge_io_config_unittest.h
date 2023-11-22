#pragma once

#include "autil/Log.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace config {

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
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeIOConfigTest, TestJsonize);
}} // namespace indexlib::config
