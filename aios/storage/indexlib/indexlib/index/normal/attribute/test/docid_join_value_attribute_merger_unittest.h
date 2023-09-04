#ifndef __INDEXLIB_DOCIDJOINVALUEATTRIBUTEMERGERTEST_H
#define __INDEXLIB_DOCIDJOINVALUEATTRIBUTEMERGERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/docid_join_value_attribute_merger.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class DocidJoinValueAttributeMergerTest : public INDEXLIB_TESTBASE
{
public:
    DocidJoinValueAttributeMergerTest();
    ~DocidJoinValueAttributeMergerTest();

    DECLARE_CLASS_NAME(DocidJoinValueAttributeMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestMerge();

    void TestMergeToMultiSegment();
    void TestSubMergeToMultiSegment();
    void TestMergeSpecialJoinValue();

private:
    void CheckResult(const std::string& segName, uint32_t segDocCount, bool isMain = true, docid_t offset = 0);
    ReclaimMapPtr CreateReclaimMap(uint32_t segDocCount, docid_t offset = 0);

    void DoTestMergeToMultiSegment(bool isMain, docid_t offset = 0);

    void DoMergeMultiSegment(bool isMain, const ReclaimMapPtr& reclaimMap,
                             const std::vector<docid_t>& mainBaseDocIds = {});
    std::vector<docid_t> LoadJoinValues(const std::string& segName, bool isMain);

private:
    std::string mRootDir;
    config::AttributeConfigPtr mMainJoinAttrConfig;
    config::AttributeConfigPtr mSubJoinAttrConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DocidJoinValueAttributeMergerTest, TestMerge);
INDEXLIB_UNIT_TEST_CASE(DocidJoinValueAttributeMergerTest, TestMergeToMultiSegment);
INDEXLIB_UNIT_TEST_CASE(DocidJoinValueAttributeMergerTest, TestSubMergeToMultiSegment);
INDEXLIB_UNIT_TEST_CASE(DocidJoinValueAttributeMergerTest, TestMergeSpecialJoinValue);
}} // namespace indexlib::index

#endif //__INDEXLIB_DOCIDJOINVALUEATTRIBUTEMERGERTEST_H
