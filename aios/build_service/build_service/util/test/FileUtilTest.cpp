#include "build_service/test/unittest.h"
#include "build_service/util/FileUtil.h"
#include "build_service/util/test/FakeFileSystem.h"
#include <string>
#include <sys/types.h>
#include <sys/stat.h>

using namespace std;

namespace build_service {
namespace util {

class FileUtilTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    void prepareData();
protected:
    std::string _dirName;
};

void FileUtilTest::setUp() {
    _dirName = "";

    string tempDir = GET_TEST_DATA_PATH();
    FileUtil::mkDir(tempDir + "dir_1/", true);
    FileUtil::writeFile(tempDir + "dir_1/file_3", "");

    FileUtil::mkDir(tempDir + "dir_1/dir_2/", true);
    FileUtil::writeFile(tempDir + "dir_1/dir_2/file_4", "");

    FileUtil::mkDir(tempDir + "dir_3/", true);
    FileUtil::mkDir(tempDir + "dir_3/empty_dir/", true);
    FileUtil::mkDir(tempDir + "empty_dir/", true);

    FileUtil::writeFile(tempDir + "file_1", "");
    FileUtil::writeFile(tempDir + "file_2", "");
}

void FileUtilTest::tearDown() {
    if (_dirName != "") {
        bool ret = FileUtil::remove(_dirName);
        ASSERT_TRUE(ret);
    }
}

void FileUtilTest::prepareData() {
    _dirName = FileUtil::joinFilePath(GET_TEST_DATA_PATH(), "testListLocalDir");
    string fileContent = "abc";

    //create files
    int32_t fileCount = 10;
    for (int32_t i = 0; i < fileCount; i++)
    {
        stringstream ss;
        ss << "file_" << i;
        string fileName = FileUtil::joinFilePath(_dirName, ss.str());
        ASSERT_TRUE(FileUtil::writeFile(fileName, fileContent));
    }

    //create a dir
    string tmpDirName = FileUtil::joinFilePath(_dirName, "tmpDir");
    ASSERT_TRUE(FileUtil::mkDir(tmpDirName));
}

TEST_F(FileUtilTest, testListLocalDir) {
    BS_LOG(DEBUG, "Begin Test!");

    prepareData();
    vector<string> files;
    ASSERT_TRUE(FileUtil::listDir(_dirName, files));
    ASSERT_EQ((size_t)11, files.size());

    ASSERT_TRUE(FileUtil::listDir(_dirName, files));
    ASSERT_EQ((size_t)11, files.size());
}

TEST_F(FileUtilTest, testListDirRecursiveWithFileOnly) {
    BS_LOG(DEBUG, "Begin Test!");
    vector<string> entryVec;

    ASSERT_TRUE(FileUtil::listDirRecursive(GET_TEST_DATA_PATH(), entryVec, true));


    ASSERT_EQ((size_t)4, entryVec.size());
    sort(entryVec.begin(), entryVec.end());
    ASSERT_EQ(string("dir_1/dir_2/file_4"), entryVec[0]);
    ASSERT_EQ(string("dir_1/file_3"), entryVec[1]);
    ASSERT_EQ(string("file_1"), entryVec[2]);
    ASSERT_EQ(string("file_2"), entryVec[3]);
}

TEST_F(FileUtilTest, testListDirRecursive) {
    BS_LOG(DEBUG, "Begin Test!");

    vector<string> entryVec;
    ASSERT_TRUE(FileUtil::listDirRecursive(GET_TEST_DATA_PATH(), entryVec, false));

    ASSERT_EQ((size_t)9, entryVec.size());
    sort(entryVec.begin(), entryVec.end());
    ASSERT_EQ(string("dir_1/"), entryVec[0]);
    ASSERT_EQ(string("dir_1/dir_2/"), entryVec[1]);
    ASSERT_EQ(string("dir_1/dir_2/file_4"), entryVec[2]);
    ASSERT_EQ(string("dir_1/file_3"), entryVec[3]);
    ASSERT_EQ(string("dir_3/"), entryVec[4]);
    ASSERT_EQ(string("dir_3/empty_dir/"), entryVec[5]);
    ASSERT_EQ(string("empty_dir/"), entryVec[6]);
    ASSERT_EQ(string("file_1"), entryVec[7]);
    ASSERT_EQ(string("file_2"), entryVec[8]);


    string path = GET_TEST_DATA_PATH() + "/test_for_dir_not_exist";
    ASSERT_TRUE(!FileUtil::listDirRecursive(path, entryVec, false));

    path = GET_TEST_DATA_PATH() + "/test_file";
    system(string("touch " + path).c_str());
    ASSERT_TRUE(!FileUtil::listDirRecursive(path, entryVec, false));
    system(string("rm " + path).c_str());
}

TEST_F(FileUtilTest, testListDirWithAbsolutePath) {
    BS_LOG(DEBUG, "Begin Test!");
    vector<string> entryVec;
    ASSERT_TRUE(FileUtil::listDirWithAbsolutePath(GET_TEST_DATA_PATH(), entryVec, false));

    ASSERT_EQ((size_t)5, entryVec.size());
    sort(entryVec.begin(), entryVec.end());
    ASSERT_EQ(GET_TEST_DATA_PATH() + string("dir_1/"), entryVec[0]);
    ASSERT_EQ(GET_TEST_DATA_PATH() + string("dir_3/"), entryVec[1]);
    ASSERT_EQ(GET_TEST_DATA_PATH() + string("empty_dir/"), entryVec[2]);
    ASSERT_EQ(GET_TEST_DATA_PATH() + string("file_1"), entryVec[3]);
    ASSERT_EQ(GET_TEST_DATA_PATH() + string("file_2"), entryVec[4]);
}

TEST_F(FileUtilTest, testSplitFileName) {
    BS_LOG(DEBUG, "Begin Test!");
    {
        const string str = "/usr/local/bin";
        string folder;
        string fileName;
        FileUtil::splitFileName(str, folder, fileName);
        ASSERT_EQ(string("/usr/local"), folder);
        ASSERT_EQ(string("bin"), fileName);
    }

    {
        string str = "./bin";
        string folder;
        string fileName;
        FileUtil::splitFileName(str, folder, fileName);
        ASSERT_EQ(string("."), folder);
        ASSERT_EQ(string("bin"), fileName);
    }

    {
        string str = "bin";
        string folder;
        string fileName;
        FileUtil::splitFileName(str, folder, fileName);
        ASSERT_EQ(string(""), folder);
        ASSERT_EQ(string("bin"), fileName);
    }

    {
        string str = "//bin";
        string folder;
        string fileName;
        FileUtil::splitFileName(str, folder, fileName);
        ASSERT_EQ(string("/"), folder);
        ASSERT_EQ(string("bin"), fileName);
    }
}


TEST_F(FileUtilTest, testWriteFileWithNoneExistPath)
{
    string path = GET_TEST_DATA_PATH() +
                  "/testWriteFileWithNoneExistPath/testWriteFileWithNoneExistPath";
    string content = "abc";
    string contentResult = "";
    ASSERT_TRUE(FileUtil::writeFile(path, content));
    ASSERT_TRUE(FileUtil::readFile(path, contentResult));
    ASSERT_EQ(content, contentResult);
}

TEST_F(FileUtilTest, testReadFile) {
    BS_LOG(DEBUG, "Begin Test!");

    string newDirPath = GET_TEST_DATA_PATH();

    FileUtil::mkDir(newDirPath, true);
    bool exist = false;
    ASSERT_TRUE(FileUtil::isExist(newDirPath, exist));
    ASSERT_TRUE(exist);
    string newFileName = newDirPath + "file_1";
    FileUtil::writeFile(newFileName, "testGetContent ok!");
    string content;
    ASSERT_TRUE(FileUtil::readFile(newFileName, content));
    ASSERT_EQ(size_t(18), content.length());
    ASSERT_EQ(string("testGetContent ok!"), content);

    FileUtil::remove(newDirPath);
}

TEST_F(FileUtilTest, testGetHostFromZkPath) {
    {
        string zkPath = "zfs:/zkHost/generation_0";
        ASSERT_EQ("", FileUtil::getHostFromZkPath(zkPath));
    }
    {
        string zkPath = "zfs://zkHost/generation_0";
        ASSERT_EQ("zkHost", FileUtil::getHostFromZkPath(zkPath));
    }
}

TEST_F(FileUtilTest, testGetPathFromZkPath) {
    {
        string zkPath = "zfs:/zkHost/generation_0";
        ASSERT_EQ("", FileUtil::getPathFromZkPath(zkPath));
    }
    {
        string zkPath = "zfs://zkHost/generation_0";
        ASSERT_EQ("/generation_0", FileUtil::getPathFromZkPath(zkPath));
    }
    {
        string zkPath = "zfs://zkHost/generation_0/partition_0_65535";
        ASSERT_EQ("/generation_0/partition_0_65535", FileUtil::getPathFromZkPath(zkPath));
    }
    {
        string zkPath = "zfs://zkHost";
        ASSERT_EQ("", FileUtil::getPathFromZkPath(zkPath));
    }
}

TEST_F(FileUtilTest, testAtomicCopy) {
    string tempDir = GET_TEST_DATA_PATH();
    string src = tempDir + "file_1";
    string dst = tempDir + "dir_1/file_1";
    {
        // tmp exist fail
        setIsExist(true);
        EXPECT_FALSE(FileUtil::atomicCopy(src, ""));
        setIsExist(false);
    }
    {
        // tmp exist
        FileUtil::writeFile(dst + ".__tmp__", "");
        EXPECT_TRUE(FileUtil::atomicCopy(src, dst));
        FileUtil::remove(dst);
    }
    {
        // tmp exist
        // rm tmp fail
        FileUtil::writeFile(dst + ".__tmp__", "");
        setRemove(true);
        EXPECT_FALSE(FileUtil::atomicCopy(src, dst));
        setRemove(false);
    }
    {
        // copy fail
        setCopy(true);
        EXPECT_FALSE(FileUtil::atomicCopy(src, dst));
        setCopy(false);
    }
    {
        // rename fail
        setRename(true);
        EXPECT_FALSE(FileUtil::atomicCopy(src, dst));
        setRename(false);
    }
    {
        EXPECT_TRUE(FileUtil::atomicCopy(src, dst));
        EXPECT_FALSE(FileUtil::atomicCopy(src, dst));
    }
}

}
}
