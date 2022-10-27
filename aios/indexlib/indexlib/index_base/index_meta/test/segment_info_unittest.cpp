#include <autil/StringUtil.h>
#include "indexlib/index_base/index_meta/test/segment_info_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"
#include "indexlib/file_system/in_mem_directory.h"
#include "indexlib/config/load_config_list.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index_base);

void SegmentInfoTest::CaseSetUp()
{
    mRoot = GET_TEST_DATA_PATH();
    mFileName = "segment_info";
}

void SegmentInfoTest::CaseTearDown()
{
}

void SegmentInfoTest::TestCaseForJson()
{
    SegmentInfo segInfo;
    CreateInfo(segInfo, 20, Locator("10"), 100, true, 2);
        
    string str = segInfo.ToString();
    SegmentInfo other;
    other.FromString(str);
    ASSERT_EQ(other, segInfo);
}

void SegmentInfoTest::CreateInfo(SegmentInfo& info, uint32_t docCount, 
                                 Locator locator, int64_t timestamp, 
                                 bool isMerged, uint32_t shardingColumnCount)
{
    info.docCount = docCount;
    info.locator = locator;
    info.timestamp = timestamp;
    info.mergedSegment = isMerged;
    info.shardingColumnCount = shardingColumnCount;
}

void SegmentInfoTest::TestCaseForStore()
{
    SegmentInfo segmentInfo;
    segmentInfo.timestamp = 2;
    segmentInfo.docCount = 3;
    segmentInfo.Store(GET_SEGMENT_DIRECTORY());
    EXPECT_THROW(segmentInfo.Store(GET_SEGMENT_DIRECTORY()), ExceptionBase);

    SegmentInfo newSegmentInfo;
    newSegmentInfo.Load(GET_SEGMENT_DIRECTORY());
    ASSERT_EQ(newSegmentInfo, segmentInfo);
}

void SegmentInfoTest::TestCaseForStoreIgnoreExist()
{
    SegmentInfo segmentInfo;
    segmentInfo.timestamp = 2;
    segmentInfo.docCount = 3;
    segmentInfo.StoreIgnoreExist(GET_SEGMENT_DIRECTORY()->GetPath() + "/segment_info");
    segmentInfo.StoreIgnoreExist(GET_SEGMENT_DIRECTORY()->GetPath() + "/segment_info");

    SegmentInfo newSegmentInfo;
    newSegmentInfo.Load(GET_SEGMENT_DIRECTORY());
    ASSERT_EQ(newSegmentInfo, segmentInfo);
}

IE_NAMESPACE_END(index_base);
 
