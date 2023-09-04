#ifndef __INDEXLIB_MERGEPLANTEST_H
#define __INDEXLIB_MERGEPLANTEST_H

#include "indexlib/common_define.h"
#include "indexlib/merger/merge_plan.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class MergePlanTest : public INDEXLIB_TESTBASE
{
public:
    MergePlanTest();
    ~MergePlanTest();

    DECLARE_CLASS_NAME(MergePlanTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAddSegment();
    void TestStoreAndLoad();

private:
    void AddOneSegment(MergePlan& mergePlan, segmentid_t segId, int64_t ts, uint32_t maxTTL, uint32_t levelId,
                       uint32_t inLevelIdx);
    std::vector<segmentid_t> GetSegIdsFromPlan(const MergePlan& mergePlan);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergePlanTest, TestAddSegment);
INDEXLIB_UNIT_TEST_CASE(MergePlanTest, TestStoreAndLoad);
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGEPLANTEST_H
