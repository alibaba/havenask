#include <ha3/util/test/FakeFileCreator.h>
#include <suez/turing/common/FileUtil.h>

using namespace std;
using namespace fslib;
using namespace fs;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, FakeFileCreator);

FakeFileCreator::FakeFileCreator() { 
}

FakeFileCreator::~FakeFileCreator() { 
}

bool FakeFileCreator::prepareRecursiveDir(const std::string &testDir) {
    string tempDir = FileUtil::normalizeDir(testDir);
    if (FileSystem::isExist(tempDir) == EC_TRUE) {
        FileSystem::remove(tempDir);
    }
    
    FileSystem::mkDir(tempDir + "dir_1/", true);
    FileUtil::writeLocalFile(tempDir + "dir_1/file_3", "");

    FileSystem::mkDir(tempDir + "dir_1/dir_2/", true);
    FileUtil::writeLocalFile(tempDir + "dir_1/dir_2/file_4", "");

    FileSystem::mkDir(tempDir + "dir_3/", true);
    FileSystem::mkDir(tempDir + "dir_3/empty_dir/", true);
    FileSystem::mkDir(tempDir + "empty_dir/", true);

    FileUtil::writeLocalFile(tempDir + "file_1", "");
    FileUtil::writeLocalFile(tempDir + "file_2", "");

    return true;
}

bool FakeFileCreator::copyDirWithoutSvn(const std::string &from, 
                                        const std::string &to)
{
    FileSystem::copy(from, to);
    vector<string> entryVec;
    FileUtil::listDirRecursive(to, entryVec, false);
    vector<string>::iterator it = entryVec.begin();
    for (; it != entryVec.end();) {
        if (it->find(".svn") != string::npos) {
            string absolutePath = FileUtil::normalizeDir(to) + *it;
            if (FileSystem::isExist(absolutePath) == EC_TRUE) {
                FileSystem::remove(absolutePath);
            }
            entryVec.erase(it);
        } else {
            it++;
        }
    }
    return true;
}

END_HA3_NAMESPACE(util);

