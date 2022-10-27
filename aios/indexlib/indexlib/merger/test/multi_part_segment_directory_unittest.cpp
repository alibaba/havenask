#include <autil/StringUtil.h>
#include <autil/StringTokenizer.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/merger/segment_directory_finder.h"
#include "indexlib/test/version_maker.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);

class MultiPartSegmentDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(MultiPartSegmentDirectoryTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }
    void TestCaseForSubDirMultiPartition()
    {
        string testdata = "1:0,2,3;0:1,3,5;1:1,10,55";
        vector<string> roots;
        vector<Version> srcVersions;
        MakeData(testdata, roots, srcVersions, true);
        config::IndexPartitionOptions options;
        MultiPartSegmentDirectory segDir(roots, srcVersions);
        segDir.Init(true);
        Version destVersion = segDir.GetVersion();
        uint32_t segCount = 0;
        SegmentDirectoryPtr subSegDir = segDir.GetSubSegmentDirectory();
        for (size_t i = 0; i < srcVersions.size(); ++i)
        {
            segCount += srcVersions[i].GetSegmentCount();
        }
        INDEXLIB_TEST_EQUAL(segCount, destVersion.GetSegmentCount());

        size_t idx = 0;
        for (size_t i = 0; i < srcVersions.size(); ++i)
        {
            for (size_t j = 0; j < srcVersions[i].GetSegmentCount(); ++j)
            {
                string actSegPath = subSegDir->GetSegmentPath(destVersion[idx++]);
                string expSegPath = SegmentDirectoryFinder::MakeSegmentPath(
                    roots[i], srcVersions[i][j], srcVersions[i], true);
                INDEXLIB_TEST_EQUAL(expSegPath, actSegPath);
            }
        }
        
    }
    void TestCaseForMultiPartition()
    {
        string testdata = "1:0,2,3;0:1,3,5;1:1,10,55";
        vector<string> roots;
        vector<Version> srcVersions;
        MakeData(testdata, roots, srcVersions);
        config::IndexPartitionOptions options;
        MultiPartSegmentDirectory segDir(roots, srcVersions);
        segDir.Init(false);

        Version destVersion = segDir.GetVersion();
        uint32_t segCount = 0;
        for (size_t i = 0; i < srcVersions.size(); ++i)
        {
            segCount += srcVersions[i].GetSegmentCount();
        }
        INDEXLIB_TEST_EQUAL(segCount, destVersion.GetSegmentCount());

        size_t idx = 0;
        for (size_t i = 0; i < srcVersions.size(); ++i)
        {
            for (size_t j = 0; j < srcVersions[i].GetSegmentCount(); ++j)
            {
                string actSegPath = segDir.GetSegmentPath(destVersion[idx++]);
                string expSegPath = SegmentDirectoryFinder::MakeSegmentPath(
                        roots[i], srcVersions[i][j], srcVersions[i]);
                INDEXLIB_TEST_EQUAL(expSegPath, actSegPath);
            }
        }
    }

    void TestCaseForListAttrPatch()
    {
        string testdata = "0:0,2,3;0:2,5,7;0:1,2,3";
        vector<string> roots;
        vector<Version> versions;
        MakeData(testdata, roots, versions);
        string attrName = "attr_name";
        string patchFiles = "2,3;3;;5;7;;2,3;3;";
        vector<vector<string> > patchs;
        MakePatchData(attrName, roots, versions, patchFiles, patchs);
        config::IndexPartitionOptions options;
        MultiPartSegmentDirectory segDir(roots, versions);
        segDir.Init(false);
        for (size_t i = 0; i < patchs.size(); ++i)
        {
            vector<pair<string,segmentid_t> > patch;
            segDir.ListAttrPatch(attrName, i, patch);
            INDEXLIB_TEST_EQUAL(patchs[i].size(), patch.size());
            for (size_t j = 0; j < patch.size(); ++j)
            {
                INDEXLIB_TEST_EQUAL(patchs[i][j], patch[j].first);
            }
        }
    }

    void TestCaseForTimestamp()
    {
        string testdata = "1:0,2,3;0:1,3,5;1:1,10,55";
        vector<string> roots;
        vector<Version> srcVersions;
        MakeData(testdata, roots, srcVersions);
        
        for (size_t i = 0; i < srcVersions.size(); i++)
        {
            srcVersions[i].SetTimestamp(i);
        }

        config::IndexPartitionOptions options;
        MultiPartSegmentDirectory segDir(roots, srcVersions);
        segDir.Init(false);

        INDEXLIB_TEST_EQUAL((int64_t)srcVersions.size() - 1,
                            segDir.GetVersion().GetTimestamp());
    }


private:
    void MakeData(const string& testdata, vector<string>& roots, 
                  vector<Version>& versions, bool needSub = false)
    {
        file_system::DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
        StringTokenizer st(testdata, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                           | StringTokenizer::TOKEN_TRIM);
        for (size_t i = 0; i < st.getNumTokens(); ++i)
        {
            StringTokenizer st1(st[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                    | StringTokenizer::TOKEN_TRIM);
            stringstream ss;
            ss << "partition_" << i << "/";
            file_system::DirectoryPtr partDirectory = 
                rootDirectory->MakeDirectory(ss.str());
            roots.push_back(partDirectory->GetPath());

            versionid_t versionId;
            StringUtil::strToInt32(st1[0].c_str(), versionId);
            Version version = VersionMaker::Make(partDirectory, versionId, 
                    st1[1], "", "", INVALID_TIMESTAMP, true);
            versions.push_back(version);
        }
    }

    void MakePatchData(const string& attrName, const vector<string>& roots, 
                       const vector<Version>& versions, const string& patchFiles, 
                       vector<vector<string> >& patchs, bool needSub = false)
    {
        string attrPath = string(ATTRIBUTE_DIR_NAME) + '/' + attrName + '/';
        StringTokenizer st(patchFiles, ";", StringTokenizer::TOKEN_TRIM);

        size_t idx = 0;
        patchs.resize(st.getNumTokens());
        for (size_t i = 0; i < versions.size(); ++i)
        {
            for (size_t j = 0; j < versions[i].GetSegmentCount(); ++j)
            { // patch for each segment
                segmentid_t oldSegId = versions[i][j];
                stringstream ss;
                ss << oldSegId << '.' << ATTRIBUTE_PATCH_FILE_NAME;
                StringTokenizer st1(st[idx], ",", StringTokenizer::TOKEN_IGNORE_EMPTY 
                        | StringTokenizer::TOKEN_TRIM);
                for (size_t k = 0; k < st1.getNumTokens(); ++k)
                {
                    segmentid_t segId;
                    StringUtil::strToInt32(st1[k].c_str(), segId);
                    string segPath = SegmentDirectoryFinder::MakeSegmentPath(
                            roots[i], segId, versions[i]);
                    string filePath = segPath + attrPath + ss.str();
                    string patchContent = "patch";
                    FileSystemWrapper::AtomicStore(filePath, patchContent);
                    patchs[idx].push_back(filePath);
                }
                idx++;
            }
        }
    }

private:
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(MultiPartSegmentDirectoryTest, TestCaseForMultiPartition);
INDEXLIB_UNIT_TEST_CASE(MultiPartSegmentDirectoryTest, TestCaseForListAttrPatch);
INDEXLIB_UNIT_TEST_CASE(MultiPartSegmentDirectoryTest, TestCaseForTimestamp);
INDEXLIB_UNIT_TEST_CASE(MultiPartSegmentDirectoryTest, TestCaseForSubDirMultiPartition);

IE_NAMESPACE_END(merger);
