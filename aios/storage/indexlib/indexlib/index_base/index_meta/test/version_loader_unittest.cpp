#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

using namespace indexlib::file_system;

namespace indexlib { namespace index_base {

class VersionLoaderTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(VersionLoaderTest);
    void CaseSetUp() override { mRootDir = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}

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
        VersionLoader::ListVersion(GET_PARTITION_DIRECTORY(), fileList);
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
        VersionLoader::ListSegments(GET_PARTITION_DIRECTORY(), dirList);
        INDEXLIB_TEST_EQUAL((size_t)5, dirList.size());
        INDEXLIB_TEST_EQUAL(string("segment_0"), dirList[0]);
        INDEXLIB_TEST_EQUAL(string("segment_1"), dirList[1]);
        INDEXLIB_TEST_EQUAL(string("segment_2"), dirList[2]);
        INDEXLIB_TEST_EQUAL(string("segment_8"), dirList[3]);
        INDEXLIB_TEST_EQUAL(string("segment_11"), dirList[4]);
    }

private:
    void CreateFile(const string& fileName) { GET_PARTITION_DIRECTORY()->Store(fileName, ""); }

    void CreateDirectory(const string& dirName) { GET_PARTITION_DIRECTORY()->MakeDirectory(dirName); }

private:
    string mRootDir;
};

INDEXLIB_UNIT_TEST_CASE(VersionLoaderTest, TestCaseForListVersion);
INDEXLIB_UNIT_TEST_CASE(VersionLoaderTest, TestCaseForListSegments);
}} // namespace indexlib::index_base
