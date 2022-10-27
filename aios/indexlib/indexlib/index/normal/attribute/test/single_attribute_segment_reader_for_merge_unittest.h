#ifndef __INDEXLIB_SINGLEATTRIBUTESEGMENTREADERFORMERGETEST_H
#define __INDEXLIB_SINGLEATTRIBUTESEGMENTREADERFORMERGETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/single_attribute_segment_reader_for_merge.h"

IE_NAMESPACE_BEGIN(index);

class SingleAttributeSegmentReaderForMergeTest : public INDEXLIB_TESTBASE {
public:
    SingleAttributeSegmentReaderForMergeTest();
    ~SingleAttributeSegmentReaderForMergeTest();
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOpenWithoutPatch();
    void TestOpenWithPatch();

private:
    void CheckReaderLoadPatch(
            const config::AttributeConfigPtr& attrConfig,
            segmentid_t segId, const std::string& expectResults);

private:
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SingleAttributeSegmentReaderForMergeTest, TestOpenWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(SingleAttributeSegmentReaderForMergeTest, TestOpenWithPatch);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLEATTRIBUTESEGMENTREADERFORMERGETEST_H
