#include "indexlib/index_base/index_meta/test/segment_info_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;
using namespace indexlibv2::framework;
using namespace indexlib::config;
using namespace indexlib::util;

namespace indexlib { namespace index_base {

void SegmentInfoTest::CaseSetUp()
{
    mRoot = GET_TEMP_DATA_PATH();
    mFileName = "segment_info";
}

void SegmentInfoTest::CaseTearDown() {}

void SegmentInfoTest::TestCaseForJson()
{
    SegmentInfo segInfo;
    CreateInfo(segInfo, 20, Locator(0, 10), 100, true, 2);

    string str = segInfo.ToString();
    SegmentInfo other;
    other.FromString(str);
    ASSERT_EQ(other, segInfo);
}

void SegmentInfoTest::CreateInfo(SegmentInfo& info, uint32_t docCount, Locator locator, int64_t timestamp,
                                 bool isMerged, uint32_t shardCount)
{
    info.docCount = docCount;
    info.SetLocator(locator);
    info.timestamp = timestamp;
    info.mergedSegment = isMerged;
    info.shardCount = shardCount;
}

void SegmentInfoTest::TestCaseForStore()
{
    file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = file_system::FSST_MEM;
    RESET_FILE_SYSTEM("ut", true /* auto mount */, fsOptions);
    SegmentInfo segmentInfo;
    segmentInfo.timestamp = 2;
    segmentInfo.docCount = 3;
    segmentInfo.Store(GET_SEGMENT_DIRECTORY());
    EXPECT_THROW(segmentInfo.Store(GET_SEGMENT_DIRECTORY()), ExceptionBase);

    SegmentInfo newSegmentInfo;
    newSegmentInfo.Load(GET_SEGMENT_DIRECTORY());
    ASSERT_EQ(newSegmentInfo, segmentInfo);
}
}} // namespace indexlib::index_base
