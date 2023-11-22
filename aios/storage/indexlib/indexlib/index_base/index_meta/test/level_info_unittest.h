#pragma once

#include "indexlib/common_define.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class LevelInfoTest : public INDEXLIB_TESTBASE
{
public:
    LevelInfoTest();
    ~LevelInfoTest();

    DECLARE_CLASS_NAME(LevelInfoTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestRemoveSegment();
    void TestGetSegmentIds();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LevelInfoTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(LevelInfoTest, TestRemoveSegment);
INDEXLIB_UNIT_TEST_CASE(LevelInfoTest, TestGetSegmentIds);
}} // namespace indexlib::index_base
