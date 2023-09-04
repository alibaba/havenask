#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/merger/merge_strategy/optimize_merge_strategy.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/test/merge_helper.h"
#include "indexlib/merger/test/segment_directory_creator.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::common;

namespace indexlib { namespace merger {

class OptimizeMergeStrategyTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(OptimizeMergeStrategyTest);
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestCaseForSetParameter();
    void TestCaseForSetParameterForMultiSegment();
    void TestCaseForOptimizeWithMaxSegmentDocCount();
    void TestCaseForOptimizeWithAfterMergeMaxDocCount();
    void TestCaseForOptimizeWithAfterMergeMaxSegmentCount();
    void TestCaseForCreateTask();
    void TestRepeatMerge();

private:
    // segmentId:isMerged#docCount[,segmentId:isMerged#docCount]
    index_base::SegmentMergeInfos CreateSegmentMergeInfos(const std::string& segMergeInfosStr);

private:
    string mRootDir;
    SegmentDirectoryPtr mSegDir;
    indexlibv2::framework::LevelInfo mLevelInfo;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(merger, OptimizeMergeStrategyTest);

INDEXLIB_UNIT_TEST_CASE(OptimizeMergeStrategyTest, TestCaseForSetParameter);
INDEXLIB_UNIT_TEST_CASE(OptimizeMergeStrategyTest, TestCaseForSetParameterForMultiSegment);
INDEXLIB_UNIT_TEST_CASE(OptimizeMergeStrategyTest, TestCaseForOptimizeWithMaxSegmentDocCount);
INDEXLIB_UNIT_TEST_CASE(OptimizeMergeStrategyTest, TestCaseForOptimizeWithAfterMergeMaxDocCount);
INDEXLIB_UNIT_TEST_CASE(OptimizeMergeStrategyTest, TestCaseForOptimizeWithAfterMergeMaxSegmentCount);
INDEXLIB_UNIT_TEST_CASE(OptimizeMergeStrategyTest, TestCaseForCreateTask);
INDEXLIB_UNIT_TEST_CASE(OptimizeMergeStrategyTest, TestRepeatMerge);
}} // namespace indexlib::merger
