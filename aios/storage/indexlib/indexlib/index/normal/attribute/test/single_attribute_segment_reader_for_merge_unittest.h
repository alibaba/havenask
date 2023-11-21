#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class SingleAttributeSegmentReaderForMergeTest : public INDEXLIB_TESTBASE
{
public:
    SingleAttributeSegmentReaderForMergeTest();
    ~SingleAttributeSegmentReaderForMergeTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOpenWithoutPatch();
    void TestOpenWithPatch();
    void TestReadNullField();

private:
    void CheckReaderLoadPatch(const config::AttributeConfigPtr& attrConfig, segmentid_t segId,
                              const std::string& expectResults);

private:
    std::string mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SingleAttributeSegmentReaderForMergeTest, TestOpenWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(SingleAttributeSegmentReaderForMergeTest, TestOpenWithPatch);
INDEXLIB_UNIT_TEST_CASE(SingleAttributeSegmentReaderForMergeTest, TestReadNullField);
}} // namespace indexlib::index
