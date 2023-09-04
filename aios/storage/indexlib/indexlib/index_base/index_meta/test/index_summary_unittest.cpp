#include "indexlib/index_base/index_meta/test/index_summary_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/deploy_index_wrapper.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, IndexSummaryTest);

IndexSummaryTest::IndexSummaryTest() {}

IndexSummaryTest::~IndexSummaryTest() {}

void IndexSummaryTest::CaseSetUp() {}

void IndexSummaryTest::CaseTearDown() {}

void IndexSummaryTest::TestSimpleProcess()
{
    auto rootDir = GET_PARTITION_DIRECTORY();
    MakeSegmentData(rootDir, 0, "f1:1;f2:2;f3:3");
    MakeSegmentData(rootDir, 1, "f4:4;f5:5;f6:6");
    MakeSegmentData(rootDir, 2, "f7:7;f8:8;f9:9");
    MakeVersion(rootDir, "1,2", 0, false);

    IndexSummary ret = IndexSummary::Load(rootDir, 0);
    ret.Commit(rootDir);
    CheckIndexSummary(ret, "1,2", "0,1,2", 45);

    MakeSegmentData(rootDir, 3, "f10:10;f11:11;f12:12");
    MakeVersion(rootDir, "1,2,3", 1, true);

    ret = IndexSummary::Load(rootDir, 1, 0);
    CheckIndexSummary(ret, "1,2,3", "0,1,2,3", -1);
}

void IndexSummaryTest::MakeSegmentData(const file_system::DirectoryPtr& rootDir, segmentid_t id,
                                       const string& fileInfos)
{
    Version v;
    string segName = v.GetNewSegmentDirName(id);
    auto segDir = rootDir->MakeDirectory(segName);
    ASSERT_NE(segDir, nullptr);

    vector<vector<string>> fileInfoStrVec;
    StringUtil::fromString(fileInfos, fileInfoStrVec, ":", ";");
    for (size_t i = 0; i < fileInfoStrVec.size(); i++) {
        assert(fileInfoStrVec[i].size() == 2);
        string fileName = fileInfoStrVec[i][0];
        int64_t len = StringUtil::numberFromString<int64_t>(fileInfoStrVec[i][1]);

        string data;
        data.resize(len);
        segDir->Store(fileName, data);
    }
    DeployIndexWrapper::DumpSegmentDeployIndex(segDir, "");
}

void IndexSummaryTest::MakeVersion(const file_system::DirectoryPtr& rootDir, const string& segIds, versionid_t id,
                                   bool createDeployIndex)
{
    Version v(id);
    vector<segmentid_t> segIdVec;
    StringUtil::fromString(segIds, segIdVec, ",");
    for (size_t i = 0; i < segIdVec.size(); i++) {
        v.AddSegment(segIdVec[i]);
    }
    v.Store(rootDir, true);

    if (createDeployIndex) {
        DeployIndexWrapper::DumpDeployMeta(rootDir, v);
    }
}

void IndexSummaryTest::CheckIndexSummary(const IndexSummary& ret, const string& usingSegIds, const string& totalSegIds,
                                         int64_t totalSize)
{
    vector<segmentid_t> segIdVec;
    StringUtil::fromString(usingSegIds, segIdVec, ",");
    ASSERT_EQ(segIdVec, ret.GetUsingSegments());

    vector<segmentid_t> totalIdVec;
    StringUtil::fromString(totalSegIds, totalIdVec, ",");

    const vector<SegmentSummary>& segSummarys = ret.GetSegmentSummarys();
    ASSERT_EQ(totalIdVec.size(), segSummarys.size());
    for (size_t i = 0; i < segSummarys.size(); i++) {
        ASSERT_EQ(totalIdVec[i], segSummarys[i].id);
    }

    if (totalSize > 0) {
        ASSERT_EQ(totalSize, ret.GetTotalSegmentSize());
    }
}
}} // namespace indexlib::index_base
