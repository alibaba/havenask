#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index_base {

class SegmentInfoTest : public INDEXLIB_TESTBASE
{
public:
    SegmentInfoTest() {}
    ~SegmentInfoTest() {}

    DECLARE_CLASS_NAME(SegmentInfoTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForJson();
    void TestCaseForStore();

private:
    void CreateInfo(SegmentInfo& info, uint32_t docCount, indexlibv2::framework::Locator locator, int64_t timestamp,
                    bool isMerged, uint32_t shardCount);

private:
    std::string mRoot;
    std::string mFileName;
    static const uint32_t STEP_SIZE = 10;
};

INDEXLIB_UNIT_TEST_CASE(SegmentInfoTest, TestCaseForJson);
INDEXLIB_UNIT_TEST_CASE(SegmentInfoTest, TestCaseForStore);
}} // namespace indexlib::index_base
