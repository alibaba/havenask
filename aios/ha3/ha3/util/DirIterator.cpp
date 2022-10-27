#include <ha3/util/DirIterator.h>
#include <suez/turing/common/FileUtil.h>
#include <fslib/fslib.h>

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(util);
 HA3_LOG_SETUP(util, DirIterator);

DirIterator::DirIterator(const std::string &dirName) {
    getFileNameList(dirName, _fileNames);
    _currentIdx = -1;
}

DirIterator::DirIterator(const std::vector<std::string>& fileList) {
    _fileNames = fileList;
    _currentIdx = -1;
}

bool DirIterator::getNextFile(std::string& nextFileName) {
    _currentIdx++;
    if((size_t)_currentIdx >= _fileNames.size()){
        return false;
    }
    
    nextFileName = _fileNames[_currentIdx];
    return true;
}

void DirIterator::getFileNameList(const string& dirName, 
                             vector<string> &fileNames) 
{
    string path = FileUtil::normalizeDir(dirName);
    FileList fileList;
    if (FileSystem::listDir(dirName, fileList) != EC_OK) {
        HA3_LOG(ERROR, "failed to listDir[%s]", dirName.c_str());
        return;
    }

    for (size_t i = 0; i < fileList.size(); ++i) {
        string fullPath = path + fileList[i];
        if (FileSystem::isFile(fullPath) == EC_TRUE) {
            fileNames.push_back(fullPath);
        }
    }
    sort(fileNames.begin(), fileNames.end());
}

END_HA3_NAMESPACE(util);

