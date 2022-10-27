#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/turing/common/FileUtil.h>
#include <sstream>
#include <ha3/util/DirIterator.h>

using namespace std;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(util);

class DirIteratorTest : public TESTBASE {
public:
    DirIteratorTest();
    ~DirIteratorTest();
public:
    void setUp();
    void tearDown();
protected:
    void testDirIterator(const std::string &path, size_t fileCount);
    std::string genFileName(const std::string &path,int i);
    std::string genDirName(const std::string &path, int i);
    void writeLocalFile(const std::string &path, size_t fileCount);
    void writeLocalDir(const std::string &path, size_t dirCount);
    void removeLocalDir(const std::string &path);
protected:
    HA3_LOG_DECLARE();
};

 HA3_LOG_SETUP(util, DirIteratorTest);


DirIteratorTest::DirIteratorTest() { 
}

DirIteratorTest::~DirIteratorTest() { 
}

void DirIteratorTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void DirIteratorTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

string DirIteratorTest::genFileName(const string& path, int i) {
    stringstream ss;
    ss << path << i << ".file";
    return ss.str();
}

string DirIteratorTest::genDirName(const string& path, int i) {
    stringstream ss;
    ss << path << i << ".dir";
    return ss.str();
}

void DirIteratorTest::writeLocalFile(const std::string& path, size_t fileCount) {
    for(size_t i = 0; i < fileCount; i++) {
        string content = "whatever content";
        ASSERT_TRUE(FileUtil::writeLocalFile(genFileName(path, i), content));
    }
}

void DirIteratorTest::writeLocalDir(const std::string& path, size_t dirCount) {
    for(size_t i = 0; i < dirCount; i++) {
        ASSERT_TRUE(FileUtil::makeLocalDir(genDirName(path, i), true));
    }
}
void DirIteratorTest::removeLocalDir(const std::string &path) {
    ASSERT_TRUE(FileUtil::removeLocalDir(path, true));
}

TEST_F(DirIteratorTest, testLocalDirIterator) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string localPath = string("testdata/testLocalDirIterator/");
    size_t fileCount = 100;
    size_t dirCount = 100;
    writeLocalFile(localPath, fileCount);
    writeLocalDir(localPath, dirCount);
    testDirIterator(localPath, fileCount);
    removeLocalDir(localPath);
}

void DirIteratorTest::testDirIterator(const std::string &path, size_t fileCount) {
    DirIterator dirIterator(path);
    ASSERT_EQ(fileCount, dirIterator.getFileCount());
    vector<string> fileNames;
    for (size_t i = 0; i < fileCount; i++) {
        fileNames.push_back(genFileName(path, i));
    }
    sort(fileNames.begin(), fileNames.end());
    string nextFileName;
    for (size_t i = 0; i < fileCount; i++) {
        ASSERT_TRUE(dirIterator.getNextFile(nextFileName));
        ASSERT_EQ(fileNames[i], nextFileName);
    }
    ASSERT_TRUE(!dirIterator.getNextFile(nextFileName));
}

END_HA3_NAMESPACE(util);
