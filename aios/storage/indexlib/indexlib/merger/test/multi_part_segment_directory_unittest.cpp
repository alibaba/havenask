#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/segment_directory_finder.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/version_maker.h"

using namespace std;
using namespace autil;

using namespace indexlib::common;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {

class MultiPartSegmentDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(MultiPartSegmentDirectoryTest);
    void CaseSetUp() override { mRootDir = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}
    void TestCaseForSubDirMultiPartition()
    {
        string testdata = "1:0,2,3;0:1,3,5;1:1,10,55";
        vector<file_system::DirectoryPtr> roots;
        vector<Version> srcVersions;
        MakeData(testdata, roots, srcVersions, true);
        config::IndexPartitionOptions options;
        MultiPartSegmentDirectory segDir(roots, srcVersions);
        segDir.Init(true);
        Version destVersion = segDir.GetVersion();
        uint32_t segCount = 0;
        SegmentDirectoryPtr subSegDir = segDir.GetSubSegmentDirectory();
        for (size_t i = 0; i < srcVersions.size(); ++i) {
            segCount += srcVersions[i].GetSegmentCount();
        }
        INDEXLIB_TEST_EQUAL(segCount, destVersion.GetSegmentCount());

        size_t idx = 0;
        for (size_t i = 0; i < srcVersions.size(); ++i) {
            for (size_t j = 0; j < srcVersions[i].GetSegmentCount(); ++j) {
                auto actSegDir = subSegDir->GetSegmentDirectory(destVersion[idx++]);
                auto expSegDir =
                    SegmentDirectoryFinder::MakeSegmentDirectory(roots[i], srcVersions[i][j], srcVersions[i], true);
                INDEXLIB_TEST_EQUAL(expSegDir->GetLogicalPath(), actSegDir->GetLogicalPath());
            }
        }
    }
    void TestCaseForMultiPartition()
    {
        string testdata = "1:0,2,3;0:1,3,5;1:1,10,55";
        vector<file_system::DirectoryPtr> roots;
        vector<Version> srcVersions;
        MakeData(testdata, roots, srcVersions);
        config::IndexPartitionOptions options;
        MultiPartSegmentDirectory segDir(roots, srcVersions);
        segDir.Init(false);

        Version destVersion = segDir.GetVersion();
        uint32_t segCount = 0;
        for (size_t i = 0; i < srcVersions.size(); ++i) {
            segCount += srcVersions[i].GetSegmentCount();
        }
        INDEXLIB_TEST_EQUAL(segCount, destVersion.GetSegmentCount());

        size_t idx = 0;
        for (size_t i = 0; i < srcVersions.size(); ++i) {
            for (size_t j = 0; j < srcVersions[i].GetSegmentCount(); ++j) {
                auto actSegPath = segDir.GetSegmentDirectory(destVersion[idx++]);
                auto expSegPath =
                    SegmentDirectoryFinder::MakeSegmentDirectory(roots[i], srcVersions[i][j], srcVersions[i]);
                INDEXLIB_TEST_EQUAL(expSegPath->GetLogicalPath(), actSegPath->GetLogicalPath());
            }
        }
    }

    void TestCaseForTimestamp()
    {
        string testdata = "1:0,2,3;0:1,3,5;1:1,10,55";
        vector<file_system::DirectoryPtr> roots;
        vector<Version> srcVersions;
        MakeData(testdata, roots, srcVersions);

        for (size_t i = 0; i < srcVersions.size(); i++) {
            srcVersions[i].SetTimestamp(i);
        }

        config::IndexPartitionOptions options;
        MultiPartSegmentDirectory segDir(roots, srcVersions);
        segDir.Init(false);

        INDEXLIB_TEST_EQUAL((int64_t)srcVersions.size() - 1, segDir.GetVersion().GetTimestamp());
    }

    void TestCaseForInit()
    {
        string testdata = "1:0,2,3;0:1,3,5;1:1,10,55";
        vector<file_system::DirectoryPtr> roots;
        vector<Version> srcVersions;
        MakeData(testdata, roots, srcVersions);

        for (size_t i = 0; i < srcVersions.size(); i++) {
            srcVersions[i].SetTimestamp(i);
        }
        segmentid_t virtualSegmentId = 0;
        for (size_t i = 0; i < srcVersions.size(); i++) {
            size_t segmentCount = srcVersions[i].GetSegmentCount();
            for (size_t segmentIdx = 0; segmentIdx < segmentCount; segmentIdx++) {
                indexlibv2::framework::SegmentStatistics segStats;
                segStats.SetSegmentId(srcVersions[i][segmentIdx]);
                std::pair<int, int> stat(virtualSegmentId, virtualSegmentId + 1);
                segStats.AddStatistic("key", stat);
                srcVersions[i].AddSegmentStatistics(segStats);
                virtualSegmentId++;
            }
        }

        config::IndexPartitionOptions options;
        MultiPartSegmentDirectory segDir(roots, srcVersions);
        segDir.Init(false);
        const auto& version = segDir.GetVersion();
        size_t segmentCount = version.GetSegmentCount();
        ASSERT_EQ(virtualSegmentId, segmentCount);
        for (size_t segmentIdx = 0; segmentIdx < segmentCount; segmentIdx++) {
            indexlibv2::framework::SegmentStatistics segmentStatistics;
            ASSERT_TRUE(version.GetSegmentStatistics(version[segmentIdx], &segmentStatistics));
            const auto& stats = segmentStatistics.GetIntegerStats();
            ASSERT_EQ(1, stats.size());
            auto it = stats.find("key");
            ASSERT_TRUE(it != stats.end());
            segmentid_t segId = segmentStatistics.GetSegmentId();
            ASSERT_EQ((std::pair<int64_t, int64_t>(segId, segId + 1)), it->second);
        }
    }

private:
    void MakeData(const string& testdata, vector<file_system::DirectoryPtr>& roots, vector<Version>& versions,
                  bool needSub = false)
    {
        file_system::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
        StringTokenizer st(testdata, ";", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            StringTokenizer st1(st[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
            stringstream ss;
            ss << "partition_" << i << "/";
            file_system::DirectoryPtr partDirectory = rootDirectory->MakeDirectory(ss.str());
            roots.push_back(partDirectory);

            versionid_t versionId;
            StringUtil::strToInt32(st1[0].c_str(), versionId);
            Version version = VersionMaker::Make(partDirectory, versionId, st1[1], "", "", INVALID_TIMESTAMP, true);
            versions.push_back(version);
        }
    }

    void MakePatchData(const string& attrName, const vector<file_system::DirectoryPtr>& roots,
                       const vector<Version>& versions, const string& patchFiles, vector<vector<string>>& patchs,
                       bool needSub = false)
    {
        string attrPath = string(index::ATTRIBUTE_DIR_NAME) + '/' + attrName + '/';
        StringTokenizer st(patchFiles, ";", StringTokenizer::TOKEN_TRIM);

        size_t idx = 0;
        patchs.resize(st.getNumTokens());
        for (size_t i = 0; i < versions.size(); ++i) {
            for (size_t j = 0; j < versions[i].GetSegmentCount(); ++j) { // patch for each segment
                segmentid_t oldSegId = versions[i][j];
                stringstream ss;
                ss << oldSegId << '.' << PATCH_FILE_NAME;
                StringTokenizer st1(st[idx], ",", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
                for (size_t k = 0; k < st1.getNumTokens(); ++k) {
                    segmentid_t segId;
                    StringUtil::strToInt32(st1[k].c_str(), segId);
                    auto segDir = SegmentDirectoryFinder::MakeSegmentDirectory(roots[i], segId, versions[i]);
                    string filePath = ss.str();
                    string patchContent = "patch";
                    segDir->MakeDirectory(attrPath)->Store(filePath, patchContent);
                    patchs[idx].push_back(segDir->GetLogicalPath() + '/' + attrPath + "/" + filePath);
                }
                idx++;
            }
        }
    }

private:
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(MultiPartSegmentDirectoryTest, TestCaseForMultiPartition);
INDEXLIB_UNIT_TEST_CASE(MultiPartSegmentDirectoryTest, TestCaseForTimestamp);
INDEXLIB_UNIT_TEST_CASE(MultiPartSegmentDirectoryTest, TestCaseForSubDirMultiPartition);
INDEXLIB_UNIT_TEST_CASE(MultiPartSegmentDirectoryTest, TestCaseForInit);
}} // namespace indexlib::merger
