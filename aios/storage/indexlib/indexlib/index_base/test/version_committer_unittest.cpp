#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/index_define.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;

namespace indexlib { namespace index_base {

class VersionCommitterTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(VersionCommitterTest);

public:
    void CaseSetUp() override { mRootDir = GET_TEMP_DATA_PATH() + "/"; }

    void CaseTearDown() override {}

    void TestCaseForCleanVersionsWithBuildMergeMerge()
    {
        const static uint32_t keepVersionCount = 2;
        string versionsStr = "0;0,1;0,1,2;0,1,2,3;0,4;5;";
        MakeVersions(versionsStr, false);
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 5));

        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 5));
    }

    void TestCaseForCleanVersionsForDeployIndexMeta()
    {
        const static uint32_t keepVersionCount = 2;
        string versionsStr = "0;0,1;0,1,2;0,1,2,3;0,4;5;";
        MakeVersions(versionsStr, true);
        INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 0));
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 5));

        INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 5));

        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 5));
    }

    void TestCaseForCleanVersionsForWhiteList()
    {
        {
            const uint32_t keepVersionCount = 3;
            const set<versionid_t>& reservedVersionSet = {0, 3};
            // v0: 0;  v1 : 0,1  v2: 2,3 ; v3: 4;  v4:5,6,7
            string versionsStr = "0;0,1;2,3;4;5,6,7;";
            MakeVersions(versionsStr, true);
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 0));

            VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount, reservedVersionSet);

            INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 0));
            INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 1));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 2));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 3));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 4));

            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 0));
            INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(mRootDir, 1));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 2));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 3));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 4));

            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 0));
            INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 2));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 3));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 4));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 5));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 6));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 7));
        }
        testDataPathTearDown();
        testDataPathSetup();
        {
            // case with empty whiteList
            const uint32_t keepVersionCount = 2;
            const set<versionid_t>& reservedVersionSet = {};
            // v0: 0;  v1 : 0,1  v2: 2,3 ; v3: 4;  v4:5,6,7
            string versionsStr = "0;0,1;2,3;4;5,6,7;";
            MakeVersions(versionsStr, true);
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 0));

            VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount, reservedVersionSet);

            INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
            INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 1));
            INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 2));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 3));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 4));

            INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(mRootDir, 0));
            INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(mRootDir, 1));
            INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(mRootDir, 2));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 3));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 4));

            INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 0));
            INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
            INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 2));
            INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 3));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 4));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 5));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 6));
            INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 7));
        }
    }

    void TestCaseForCleanVersionsForWhiteListWithFileSystem()
    {
        {
            const uint32_t keepVersionCount = 3;
            const set<versionid_t>& reservedVersionSet = {0, 3};
            // v0: 0;  v1 : 0,1  v2: 2,3 ; v3: 4;  v4:5,6,7
            string versionsStr = "0;0,1;2,3;4;5,6,7;";
            MakeVersions(versionsStr, true);
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(mRootDir, 0));
            file_system::DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();

            VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount, reservedVersionSet);

            INDEXLIB_TEST_TRUE(IsVersionFileExist(rootDir, 0));
            INDEXLIB_TEST_TRUE(!IsVersionFileExist(rootDir, 1));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(rootDir, 2));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(rootDir, 3));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(rootDir, 4));

            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(rootDir, 0));
            INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(rootDir, 1));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(rootDir, 2));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(rootDir, 3));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(rootDir, 4));

            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 0));
            INDEXLIB_TEST_TRUE(!IsSegmentExist(rootDir, 1));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 2));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 3));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 4));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 5));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 6));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 7));
        }
        testDataPathTearDown();
        testDataPathSetup();
        {
            // case with empty whiteList
            const uint32_t keepVersionCount = 2;
            const set<versionid_t>& reservedVersionSet = {};
            // v0: 0;  v1 : 0,1  v2: 2,3 ; v3: 4;  v4:5,6,7
            string versionsStr = "0;0,1;2,3;4;5,6,7;";
            MakeVersions(versionsStr, true);
            file_system::DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(rootDir, 0));
            VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount, reservedVersionSet);

            INDEXLIB_TEST_TRUE(!IsVersionFileExist(rootDir, 0));
            INDEXLIB_TEST_TRUE(!IsVersionFileExist(rootDir, 1));
            INDEXLIB_TEST_TRUE(!IsVersionFileExist(rootDir, 2));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(rootDir, 3));
            INDEXLIB_TEST_TRUE(IsVersionFileExist(rootDir, 4));

            INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(rootDir, 0));
            INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(rootDir, 1));
            INDEXLIB_TEST_TRUE(!IsDeployIndexMetaExist(rootDir, 2));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(rootDir, 3));
            INDEXLIB_TEST_TRUE(IsDeployIndexMetaExist(rootDir, 4));

            INDEXLIB_TEST_TRUE(!IsSegmentExist(rootDir, 0));
            INDEXLIB_TEST_TRUE(!IsSegmentExist(rootDir, 1));
            INDEXLIB_TEST_TRUE(!IsSegmentExist(rootDir, 2));
            INDEXLIB_TEST_TRUE(!IsSegmentExist(rootDir, 3));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 4));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 5));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 6));
            INDEXLIB_TEST_TRUE(IsSegmentExist(rootDir, 7));
        }
    }

    void TestCaseForCleanVersionsWithBuildMergeBuild()
    {
        const static uint32_t keepVersionCount = 2;
        string versionsStr = "0,1,2,3;0,4;0,4,5,6;";
        MakeVersions(versionsStr, false);
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 2));

        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 5));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 6));
    }

    void TestCaseForCleanVersionsWithNoClean()
    {
        const static uint32_t keepVersionCount = 3;
        string versionsStr = "0,1,2,3;0,4;0,4,5,6;";
        MakeVersions(versionsStr, false);
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 2));

        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 5));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 6));
    }

    void TestCaseForCleanVersionsWithInvalidKeepCount()
    {
        const static uint32_t keepVersionCount = 0;
        string versionsStr = "0,1,2,3;0,4;0,4,5,6;";
        MakeVersions(versionsStr, false);
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 2));

        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 5));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 6));
    }

    void TestCaseForCleanVersionsWithIncontinuous()
    {
        const static uint32_t keepVersionCount = 2;
        string versionsStr = "0,1,2,3;;0,5;0,5,6,7;";
        MakeVersions(versionsStr, false);
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 3));

        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 5));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 6));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 7));
    }

    void TestCaseForCleanVersionsWithManyIncontinuous()
    {
        const static uint32_t keepVersionCount = 3;
        string versionsStr = "0,1,4,5;;0,4,5;;0,5;;0,6;0,5,6,7;";
        MakeVersions(versionsStr, false);
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 5));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 6));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 7));

        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 5));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 6));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 7));
    }

    void TestCaseForCleanVersionsWithInvalidSegments()
    {
        const static uint32_t keepVersionCount = 2;

        string versionsStr = "0,1;2,3;;7,8,9;";
        MakeVersions(versionsStr, false);

        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_4_level_0");
        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_5_level_0");
        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_10_level_0");

        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 3));

        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 4));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 5));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 6));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 7));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 8));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 9));
        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 10));
    }

    void TestCaseForCleanVersionWithKeepOneEmptyVersion()
    {
        const static uint32_t keepVersionCount = 1;

        Version version0(0);
        version0.AddSegment(0);
        version0.AddSegment(1);

        Version version1(1);
        version1.AddSegment(2);
        version1.AddSegment(3);

        Version version2(2);

        // make segment directory
        for (uint32_t i = 0; i < 4; i++) {
            string segPath = SEGMENT_FILE_NAME_PREFIX + StringUtil::toString(i) + "_level_0/";
            GET_PARTITION_DIRECTORY()->MakeDirectory(segPath);
        }

        GET_PARTITION_DIRECTORY()->Store("version.0", version0.ToString());
        GET_PARTITION_DIRECTORY()->Store("version.1", version1.ToString());
        GET_PARTITION_DIRECTORY()->Store("version.2", version2.ToString());

        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 2));

        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 1));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 2));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 3));
        INDEXLIB_TEST_TRUE(!IsSegmentExist(mRootDir, 4));
    }

    void TestCaseForCleanVersionWithKeepAllEmptyVersion()
    {
        const static uint32_t keepVersionCount = 1;
        Version version0(0);
        Version version1(1);

        GET_PARTITION_DIRECTORY()->Store("version.0", version0.ToString());
        GET_PARTITION_DIRECTORY()->Store("version.1", version1.ToString());

        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_0_level_0/");

        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount);

        INDEXLIB_TEST_TRUE(!IsVersionFileExist(mRootDir, 0));
        INDEXLIB_TEST_TRUE(IsVersionFileExist(mRootDir, 1));

        INDEXLIB_TEST_TRUE(IsSegmentExist(mRootDir, 0));
    }

    void TestCaseForCleanVersionsWithKeepVersionIdFirst()
    {
        const static uint32_t keepVersionCount = 1;
        string versionsStr = "0,1,2,3;0,4;0,5,6;";
        MakeVersions(versionsStr, false);
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount, {0});

        EXPECT_TRUE(IsVersionFileExist(mRootDir, 0));
        EXPECT_TRUE(!IsVersionFileExist(mRootDir, 1));
        EXPECT_TRUE(IsVersionFileExist(mRootDir, 2));

        EXPECT_TRUE(IsSegmentExist(mRootDir, 0));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 1));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 2));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 3));
        EXPECT_TRUE(!IsSegmentExist(mRootDir, 4));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 5));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 6));
    }

    void TestCaseForCleanVersionsWithKeepVersionIdLast()
    {
        const static uint32_t keepVersionCount = 1;
        string versionsStr = "0,1,2,3;0,4;0,5,6;";
        MakeVersions(versionsStr, false);
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount, {2});

        EXPECT_TRUE(!IsVersionFileExist(mRootDir, 0));
        EXPECT_TRUE(!IsVersionFileExist(mRootDir, 1));
        EXPECT_TRUE(IsVersionFileExist(mRootDir, 2));

        EXPECT_TRUE(IsSegmentExist(mRootDir, 0));
        EXPECT_TRUE(!IsSegmentExist(mRootDir, 1));
        EXPECT_TRUE(!IsSegmentExist(mRootDir, 2));
        EXPECT_TRUE(!IsSegmentExist(mRootDir, 3));
        EXPECT_TRUE(!IsSegmentExist(mRootDir, 4));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 5));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 6));
    }

    void TestCaseForCleanVersionsWithKeepVersionIdMiddle()
    {
        const static uint32_t keepVersionCount = 2;
        string versionsStr = "0,1,2,3;0,4;0,5,6;";
        MakeVersions(versionsStr, false);
        VersionCommitter::CleanVersions(GET_PARTITION_DIRECTORY(), keepVersionCount, {1});

        EXPECT_TRUE(!IsVersionFileExist(mRootDir, 0));
        EXPECT_TRUE(IsVersionFileExist(mRootDir, 1));
        EXPECT_TRUE(IsVersionFileExist(mRootDir, 2));

        EXPECT_TRUE(IsSegmentExist(mRootDir, 0));
        EXPECT_TRUE(!IsSegmentExist(mRootDir, 1));
        EXPECT_TRUE(!IsSegmentExist(mRootDir, 2));
        EXPECT_TRUE(!IsSegmentExist(mRootDir, 3));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 4));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 5));
        EXPECT_TRUE(IsSegmentExist(mRootDir, 6));
    }

    // versionIdsStr: EXIST1,EXIST2...;NOTEXIST1,NOTEXIST12...
    // segemntIdsStr: EXIST1,EXIST2...;NOTEXIST1,NOTEXIST12...
    bool CheckVersionAndSegment(const std::string& versionIdsStr, const std::string& segmentIdsStr)
    {
        vector<vector<versionid_t>> versionIds;
        StringUtil::fromString(versionIdsStr, versionIds, ",", ";");
        assert(2 == versionIds.size());
        for (auto versionId : versionIds[0]) {
            if (!IsVersionFileExist(mRootDir, versionId)) {
                EXPECT_TRUE(false) << "Need Exist: " << versionId;
                return false;
            }
        }
        for (auto versionId : versionIds[1]) {
            if (IsVersionFileExist(mRootDir, versionId)) {
                EXPECT_TRUE(false) << "Need Non-Exist: " << versionId;
                return false;
            }
        }

        vector<vector<segmentid_t>> segmentIds;
        StringUtil::fromString(segmentIdsStr, segmentIds, ",", ";");
        assert(2 == segmentIds.size());
        for (auto segmentId : segmentIds[0]) {
            if (!IsSegmentExist(mRootDir, segmentId)) {
                EXPECT_TRUE(false) << "Need Exist: " << segmentId;
                return false;
            }
        }
        for (auto segmentId : segmentIds[1]) {
            if (IsSegmentExist(mRootDir, segmentId)) {
                EXPECT_TRUE(false) << "Need Non-Exist: " << segmentId;
                return false;
            }
        }
        return true;
    }

    void TestCaseForCleanVersionAndBefore()
    {
        MakeVersions("0,1,2,3;0,4;0,5,6;", false);
        ASSERT_TRUE(VersionCommitter::CleanVersionAndBefore(GET_PARTITION_DIRECTORY(), -1));
        ASSERT_TRUE(CheckVersionAndSegment("0,1,2;,", "0,1,2,3,4,5,6;,"));

        ASSERT_TRUE(VersionCommitter::CleanVersionAndBefore(GET_PARTITION_DIRECTORY(), 0));
        ASSERT_TRUE(CheckVersionAndSegment("1,2;0", "0,4,5,6;1,2,3"));

        ASSERT_TRUE(VersionCommitter::CleanVersionAndBefore(GET_PARTITION_DIRECTORY(), 1));
        ASSERT_TRUE(CheckVersionAndSegment("2;0,1", "0,5,6;1,2,3,4"));

        ASSERT_FALSE(VersionCommitter::CleanVersionAndBefore(GET_PARTITION_DIRECTORY(), 2));
        ASSERT_TRUE(CheckVersionAndSegment("2;0,1", "0,5,6;1,2,3,4"));

        testDataPathTearDown();
        testDataPathSetup();
        MakeVersions("0;1;2;3;4;5;", false);
        ASSERT_TRUE(CheckVersionAndSegment("0,1,2,3,4,5;,", "0,1,2,3,4,5;,"));

        ASSERT_FALSE(VersionCommitter::CleanVersionAndBefore(GET_PARTITION_DIRECTORY(), 5));
        ASSERT_TRUE(CheckVersionAndSegment("0,1,2,3,4,5;,", "0,1,2,3,4,5;,"));

        ASSERT_TRUE(VersionCommitter::CleanVersionAndBefore(GET_PARTITION_DIRECTORY(), 4));
        ASSERT_TRUE(CheckVersionAndSegment("5;0,1,2,3,4", "5;0,1,2,3,4"));
    }

    void TestCaseForTargetVersionExist()
    {
        Version targetVersion(1);
        targetVersion.AddSegment(1);
        targetVersion.AddSegment(2);
        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_1_level_0/");
        GET_PARTITION_DIRECTORY()->MakeDirectory("segment_2_level_0/");
        GET_PARTITION_DIRECTORY()->Store("segment_1_level_0/deploy_index", "");
        GET_PARTITION_DIRECTORY()->Store("segment_2_level_0/deploy_index", "");
        VersionCommitter committer(GET_PARTITION_DIRECTORY(), targetVersion, 2);

        GET_PARTITION_DIRECTORY()->Store("version.1", "aaa");

        ASSERT_NO_THROW(committer.Commit());
    }

    void MakeVersions(const string& versionsStr, bool deployMeta)
    {
        MakeVersionsWithFileSystem(versionsStr, deployMeta);
    }

    void MakeVersionsWithFileSystem(const string& versionsStr, bool deployMeta)
    {
        versionid_t versionId = 0;
        StringTokenizer st(versionsStr, ";", StringTokenizer::TOKEN_TRIM);
        file_system::DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
        for (size_t i = 0; i < st.getNumTokens(); ++i) {
            bool hasTempDir = false;
            Version version(versionId++);

            StringTokenizer st2(st[i], ",", StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);

            for (size_t j = 0; j < st2.getNumTokens(); ++j) {
                segmentid_t segmentId;
                StringUtil::strToInt32(st2[j].c_str(), segmentId);
                version.AddSegment(segmentId);
                string segmentDir = version.GetSegmentDirName(segmentId);
                string absPath = string("") + version.GetSegmentDirName(segmentId) + "/";
                if (absPath.find(SEGMENT_TEMP_DIR_SUFFIX) != string::npos) {
                    hasTempDir = true;
                }
                if (rootDir->IsExist(segmentDir)) {
                    continue;
                }
                rootDir->MakeDirectory(segmentDir);
            }
            if (!hasTempDir && i < st.getNumTokens() - 1) {
                if (version.GetSegmentCount() > 0) {
                    string content = version.ToString();
                    stringstream ss;
                    ss << VERSION_FILE_NAME_PREFIX << "." << version.GetVersionId();
                    rootDir->Store(ss.str(), content, WriterOption::AtomicDump());
                    if (deployMeta) {
                        stringstream ss1;
                        ss1 << DEPLOY_META_FILE_NAME_PREFIX << "." << version.GetVersionId();
                        rootDir->Store(ss1.str(), "", WriterOption::AtomicDump());
                    }
                }
            }
        }
    }

private:
    bool IsVersionFileExist(const string& rootDir, versionid_t versionId)
    {
        stringstream ss;
        ss << rootDir << VERSION_FILE_NAME_PREFIX << "." << versionId;
        return FslibWrapper::IsExist(ss.str()).GetOrThrow();
    }

    bool IsDeployIndexMetaExist(const string& rootDir, versionid_t versionId)
    {
        stringstream ss;
        ss << rootDir << DEPLOY_META_FILE_NAME_PREFIX << "." << versionId;
        return FslibWrapper::IsExist(ss.str()).GetOrThrow();
    }

    bool IsSegmentExist(const string& rootDir, segmentid_t segmentId)
    {
        stringstream ss;
        ss << rootDir << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0/";
        return FslibWrapper::IsExist(ss.str()).GetOrThrow();
    }

    bool IsVersionFileExist(const file_system::DirectoryPtr& rootDir, versionid_t versionId)
    {
        stringstream ss;
        ss << VERSION_FILE_NAME_PREFIX << "." << versionId;
        return rootDir->IsExist(ss.str());
    }

    bool IsDeployIndexMetaExist(const file_system::DirectoryPtr& rootDir, versionid_t versionId)
    {
        stringstream ss;
        ss << DEPLOY_META_FILE_NAME_PREFIX << "." << versionId;
        return rootDir->IsExist(ss.str());
    }

    bool IsSegmentExist(const file_system::DirectoryPtr& rootDir, segmentid_t segmentId)
    {
        stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0/";
        return rootDir->IsExist(ss.str());
    }

private:
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithBuildMergeMerge);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsForDeployIndexMeta);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithBuildMergeBuild);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithNoClean);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithInvalidKeepCount);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithIncontinuous);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithManyIncontinuous);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithInvalidSegments);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionWithKeepOneEmptyVersion);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionWithKeepAllEmptyVersion);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithKeepVersionIdFirst);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithKeepVersionIdLast);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsWithKeepVersionIdMiddle);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForTargetVersionExist);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionAndBefore);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsForWhiteList);
INDEXLIB_UNIT_TEST_CASE(VersionCommitterTest, TestCaseForCleanVersionsForWhiteListWithFileSystem);
}} // namespace indexlib::index_base
