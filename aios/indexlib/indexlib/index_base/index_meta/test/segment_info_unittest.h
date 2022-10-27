#ifndef SEGMENT_INFOS_TEST
#define SEGMENT_INFOS_TEST

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/segment_info.h" 

IE_NAMESPACE_BEGIN(index_base);

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
    void TestCaseForStoreIgnoreExist();
private:
    void CreateInfo(SegmentInfo& info, uint32_t docCount, 
                    document::Locator locator, int64_t timestamp, bool isMerged,
                    uint32_t shardingColumnCount);

private:
    std::string mRoot;
    std::string mFileName;
    static const uint32_t STEP_SIZE = 10;
};

INDEXLIB_UNIT_TEST_CASE(SegmentInfoTest, TestCaseForJson);
INDEXLIB_UNIT_TEST_CASE(SegmentInfoTest, TestCaseForStore);
INDEXLIB_UNIT_TEST_CASE(SegmentInfoTest, TestCaseForStoreIgnoreExist);

IE_NAMESPACE_END(index_base);

#endif //SEGMENT_INFOS_TEST
