#include "indexlib/index_base/index_meta/test/progress_synchronizer_unittest.h"

#include "indexlib/document/index_locator.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"

using namespace std;
using namespace autil;
using namespace indexlib::document;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, ProgressSynchronizerTest);

ProgressSynchronizerTest::ProgressSynchronizerTest() {}

ProgressSynchronizerTest::~ProgressSynchronizerTest() {}

void ProgressSynchronizerTest::CaseSetUp() {}

void ProgressSynchronizerTest::CaseTearDown() {}

void ProgressSynchronizerTest::TestSimpleProcess()
{
    {
        vector<Version> versions;
        MakeVersions("9,0,10;10,0,11;8,0,7", versions);
        ProgressSynchronizer ps;
        ps.Init(versions);
        ASSERT_EQ((int64_t)10, ps.GetTimestamp());
        CheckLocator(ps, 0, 7);
    }
    {
        vector<SegmentInfo> segInfos;
        MakeSegmentInfos("20,0,21;10,1,19;30,1,17", segInfos);
        ProgressSynchronizer ps;
        ps.Init(segInfos);
        ASSERT_EQ((int64_t)30, ps.GetTimestamp());
        CheckLocator(ps, 1, 17);
    }
    {
        vector<SegmentInfo> segInfos;
        MakeSegmentInfos("20,0,10;10,1,19;30,1,17;20,1,20;10,1,11;30,1,25", segInfos);
        ProgressSynchronizer ps;
        ps.Init(segInfos);
        ASSERT_EQ((int64_t)30, ps.GetTimestamp());
        CheckLocator(ps, 1, 11);
    }
}

// str: ts,src,offset;ts,src,offset
void ProgressSynchronizerTest::MakeVersions(const string& str, vector<Version>& versions)
{
    vector<vector<int64_t>> tmp;
    StringUtil::fromString(str, tmp, ",", ";");
    for (auto versionInfo : tmp) {
        assert(versionInfo.size() == 3);
        Version version(0);
        version.SetTimestamp(versionInfo[0]);
        IndexLocator indexLocator(versionInfo[1], versionInfo[2], /*userData=*/"");
        Locator locator;
        locator.SetLocator(indexLocator.toString());
        version.SetLocator(locator);
        versions.push_back(version);
    }
}

// str: ts,src,offset;ts,src,offset
void ProgressSynchronizerTest::MakeSegmentInfos(const string& str, vector<SegmentInfo>& segInfos)
{
    vector<vector<int64_t>> tmp;
    StringUtil::fromString(str, tmp, ",", ";");
    for (auto segInfo : tmp) {
        assert(segInfo.size() == 3);
        SegmentInfo segmentInfo;
        segmentInfo.timestamp = segInfo[0];

        indexlibv2::framework::Locator locator(segInfo[1], segInfo[2]);
        segmentInfo.SetLocator(locator);
        segInfos.push_back(segmentInfo);
    }
}

void ProgressSynchronizerTest::CheckLocator(const ProgressSynchronizer& ps, int64_t src, int64_t offset)
{
    IndexLocator indexLocator;
    indexLocator.fromString(ps.GetLocator().GetLocator());
    ASSERT_EQ((uint64_t)src, indexLocator.getSrc());
    ASSERT_EQ(offset, indexLocator.getOffset());
}
}} // namespace indexlib::index_base
