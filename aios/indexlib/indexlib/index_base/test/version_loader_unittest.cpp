#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/storage/file_system_wrapper.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(index_base);

class VersionLoaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(VersionLoaderTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

    void TestCaseForListVersion()
    {
        CreateFile("_version.1");
        CreateFile("version.1.bak");
        CreateFile("version.1_bak");
        CreateFile("version.1~");
        CreateFile("version.2");
        CreateFile("version.12");
        CreateFile("version.42");
        CreateFile("version.11");

        FileList fileList;
        VersionLoader::ListVersion(mRootDir, fileList);
        INDEXLIB_TEST_EQUAL(string("version.2"), fileList[0]);
        INDEXLIB_TEST_EQUAL(string("version.11"), fileList[1]);
        INDEXLIB_TEST_EQUAL(string("version.12"), fileList[2]);
        INDEXLIB_TEST_EQUAL(string("version.42"), fileList[3]);
        INDEXLIB_TEST_EQUAL((size_t)4, fileList.size());
    }

    void TestCaseForListSegments()
    {
        CreateDirectory("segment_0");
        CreateDirectory("segment_0~");
        CreateDirectory("segment_0_bak");
        CreateDirectory("segment_1");
        CreateDirectory("segment_11");
        CreateDirectory("segment_2");
        CreateDirectory("segment_0.bak");
        CreateDirectory("_segment_2");
        CreateFile("version.2");
        CreateFile("segmentinfo_2");
        CreateDirectory("segment_8");

        FileList dirList;
        VersionLoader::ListSegments(mRootDir, dirList);
        INDEXLIB_TEST_EQUAL((size_t)5, dirList.size());
        INDEXLIB_TEST_EQUAL(string("segment_0"), dirList[0]);
        INDEXLIB_TEST_EQUAL(string("segment_1"), dirList[1]);
        INDEXLIB_TEST_EQUAL(string("segment_2"), dirList[2]);
        INDEXLIB_TEST_EQUAL(string("segment_8"), dirList[3]);
        INDEXLIB_TEST_EQUAL(string("segment_11"), dirList[4]);
    }


private:
    void CreateFile(const string& fileName)
    {
        FileWrapper* file = FileSystemWrapper::OpenFile(
                mRootDir + fileName, WRITE);
        if (file)
        {
            file->Close();
        }
        delete file;
    }

    void CreateDirectory(const string& dirName)
    {
        FileSystemWrapper::MkDir(mRootDir + dirName, false);
    }

private:
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(VersionLoaderTest, TestCaseForListVersion);
INDEXLIB_UNIT_TEST_CASE(VersionLoaderTest, TestCaseForListSegments);

IE_NAMESPACE_END(index_base);
