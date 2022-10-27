#include "build_service/util/FileUtil.h"
#include <fslib/fslib.h>
#include <deque>
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

namespace build_service {
namespace util {

BS_LOG_SETUP(util, FileUtil);

const std::string FileUtil::ZFS_PREFIX = "zfs://";
const size_t FileUtil::ZFS_PREFIX_LEN = 6;
const std::string FileUtil::TMP_SUFFIX = ".__tmp__";
const std::string FileUtil::BAK_SUFFIX = ".__bak__";

bool FileUtil::readFile(const string &filePath, string &content) {
    return fslib::EC_OK == FileSystem::readFile(filePath, content);
}

bool FileUtil::writeFile(const string& filePath, const string &content) {
    return fslib::EC_OK == FileSystem::writeFile(filePath, content);
}

bool FileUtil::listDir(const string &path,
                       vector<string> &fileList)
{
    // workaround for zk, fslib not support list dir by entry meta now
    fslib::ErrorCode errorCode = FileSystem::listDir(path, fileList);
    if (errorCode != fslib::EC_OK) {
        string errorMsg = "List dir failed, path [" + path + "], error["
                          + FileSystem::getErrorString(errorCode) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool FileUtil::listDirWithAbsolutePath(const string &path,
                                       vector<string> &entryVec,
                                       bool fileOnly)
{
    if (!listDir(path, entryVec, fileOnly, false)) {
        return false;
    }
    for (size_t i = 0; i < entryVec.size(); ++i) {
        entryVec[i] = FileUtil::joinFilePath(path, entryVec[i]);
    }
    return true;
}

bool FileUtil::listDirRecursive(const string &path,
                                vector<string> &entryVec,
                                bool fileOnly)
{
    return listDir(path, entryVec, fileOnly, true);
}

bool FileUtil::listDirRecursiveWithAbsolutePath(const string &path,
        vector<string> &entryVec, bool fileOnly)
{
    if (!listDir(path, entryVec, fileOnly, true)) {
        return false;
    }

    for (size_t i = 0; i < entryVec.size(); ++i) {
        entryVec[i] = FileUtil::joinFilePath(path, entryVec[i]);
    }
    return true;
}

bool FileUtil::listDir(const string &path,
                       vector<string>& entryVec,
                       bool fileOnly,
                       bool recursive)
{
    entryVec.clear();

    deque<string> pathList;
    string relativeDir;
    do {
        EntryList tmpList;
        string absoluteDir = FileUtil::joinFilePath(path, relativeDir);
        fslib::ErrorCode errorCode =
            FileSystem::listDir(absoluteDir, tmpList);
        if (errorCode != fslib::EC_OK) {
            string errorMsg = "List dir recursive failed, path ["
                              + absoluteDir + "], error["
                              + FileSystem::getErrorString(errorCode) + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        EntryList::iterator it = tmpList.begin();
        for (; it != tmpList.end(); ++it) {
            string tmpName = FileUtil::joinFilePath(relativeDir, (*it).entryName);
            if ((*it).isDir) {
                if (recursive) {
                    pathList.push_back(tmpName);
                }
                if (!fileOnly) {
                    entryVec.push_back(normalizeDir(tmpName));
                }
            } else {
                entryVec.push_back(tmpName);
            }
        }
        if (!recursive) {
            return true;
        }
        if (pathList.empty()) {
            break;
        }
        relativeDir = pathList.front();
        pathList.pop_front();
    } while (true);

    return true;
}


bool FileUtil::mkDir(const string &dirPath, bool recursive) {
    ErrorCode ec = FileSystem::mkDir(dirPath, recursive);
    if (ec != EC_OK) {
        BS_LOG(DEBUG, "mkdir [%s] failed.", dirPath.c_str());
        return false;
    }
    return true;
}

bool FileUtil::remove(const string &path) {
    ErrorCode ec = FileSystem::remove(path);
    if (ec != EC_OK) {
        BS_LOG(DEBUG, "remove [%s] failed.", path.c_str());
        return false;
    }
    return true;
}

bool FileUtil::removeIfExist(const string &path) {
    ErrorCode ec = FileSystem::isExist(path);
    if (ec != EC_TRUE && ec != EC_FALSE) {
        string errorMsg = "failed to check whether " + path + " exist";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (ec == EC_FALSE) {
        return true;
    }
    return remove(path);
}

bool FileUtil::isDir(const string &filePath, bool &dirExist) {
    fslib::ErrorCode code = FileSystem::isDirectory(filePath);
    if (code != EC_TRUE && code != EC_FALSE && code != EC_NOENT) {
        string errorMsg = "failed to check whether [" + filePath + "] is directory";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        BS_LOG(ERROR, "file system error string: %s", FileSystem::getErrorString(code).c_str());
        return false;
    }
    dirExist = (code == EC_TRUE);
    return true;
}

bool FileUtil::isFile(const string &filePath, bool &fileExist) {
    fslib::ErrorCode code = FileSystem::isFile(filePath);
    if (code != EC_TRUE && code != EC_FALSE && code != EC_NOENT) {
        string errorMsg = "failed to check whether [" + filePath + "] is file";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    fileExist = (code == EC_TRUE);
    return true;
}

bool FileUtil::isExist(const string &path, bool &exist) {
    ErrorCode ec = FileSystem::isExist(path);
    if (ec != EC_TRUE && ec != EC_FALSE) {
        string errorMsg = "failed to check whether [" + path + "] exist.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    exist = (ec == EC_TRUE);
    return true;
}

string FileUtil::joinFilePath(const string &path, const string &file)
{
    if (path.empty()) {
        return file;
    }

    if (path[path.length() - 1] == '/') {
        return path + file;
    }

    return path + '/' + file;
}

string FileUtil::normalizeDir(const string &dirName)
{
    string tmpDirName = dirName;
    if (!tmpDirName.empty() && *(tmpDirName.rbegin()) != '/') {
        tmpDirName += "/";
    }
    return tmpDirName;
}

// TODO: ut, move to fslib
void FileUtil::splitFileName(const string &input,
                   string &folder, string &fileName)
{
    size_t found;
    string inputTmp = input;
    while(!inputTmp.empty() && inputTmp.back() == '/'){
        inputTmp.pop_back();
    }
    found = inputTmp.find_last_of("/\\");
    if (found == string::npos) {
        fileName = inputTmp;
        return;
    }
    folder = inputTmp.substr(0,found);
    fileName = inputTmp.substr(found+1);
}

string FileUtil::getHostFromZkPath(const string &zkPath) {
    if (!StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        string errorMsg = "zkPath[" + zkPath + "] is not invalid.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return "";
    }
    string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    return tmpStr.substr(0, tmpStr.find("/"));
}

string FileUtil::getPathFromZkPath(const string &zkPath) {
    if (!StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        string errorMsg = "zkPath[" + zkPath + "] is not invalid.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return "";
    }
    string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    string::size_type pos = tmpStr.find("/");
    if (pos == string::npos) {
        return "";
    }
    return tmpStr.substr(pos);
}

string FileUtil::getParentDir(const string &dir) {
    if (dir.empty()) {
        return "";
    }
    if (dir == "/") {
        return dir;
    }
    size_t delimPos = string::npos;

    if ('/' == *(dir.rbegin())) {
        //the last charactor is '/', then rfind from the next char
        delimPos = dir.rfind('/', dir.size() - 2);
    } else {
        delimPos = dir.rfind('/');
    }

    if (string::npos == delimPos) {
        //not found '/', than parent dir is null
        return "";
    }

    string parentDir = dir.substr(0, delimPos);
    return parentDir;
}

bool FileUtil::getFileLength(const string &filePath, int64_t &fileLength,
                             bool needPrintLog) {
    fslib::FileMeta fileMeta;
    fslib::ErrorCode ret = FileSystem::getFileMeta(filePath, fileMeta);
    if (ret != fslib::EC_OK) {
        if (needPrintLog) {
            string errorMsg = "getFileMeta for [" + filePath + "] failed";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
        }
        return false;
    }
    fileLength = fileMeta.fileLength;
    return true;
}

bool FileUtil::readWithBak(const std::string filepath, std::string &content) {
    auto ec = FileSystem::isExist(filepath);
    if (EC_TRUE == ec) {
        if (EC_OK == FileSystem::readFile(filepath, content)) {
            return true;
        }
    } else if (EC_FALSE == ec) {
        auto backFile = filepath + BAK_SUFFIX;
        ec = FileSystem::isExist(backFile);
        if (EC_TRUE == ec) {
            if (EC_OK == FileSystem::readFile(backFile, content)) {
                BS_LOG(INFO, "read from bak [%s]", backFile.c_str());
                return true;
            }
        } else if (EC_FALSE == ec) {
            BS_LOG(INFO, "file and bak %s not exist", backFile.c_str());
            return true;
        }
    }
    return false;
}

bool FileUtil::writeWithBak(const std::string filepath, const std::string &content) {
    auto ec = FileSystem::isExist(filepath);
    if (EC_TRUE == ec) {
        auto backFile = filepath + BAK_SUFFIX;
        if (EC_FALSE != FileSystem::isExist(backFile) &&
            EC_OK != FileSystem::remove(backFile))
        {
            return false;
        }
        if (EC_OK != FileSystem::move(filepath, backFile)) {
            return false;
        }
    } else if (EC_FALSE != ec) {
        return false;
    }
    auto tmpFile = filepath + TMP_SUFFIX;
    if (EC_OK != FileSystem::writeFile(tmpFile, content)) {
        return false;
    }
    if (EC_OK != FileSystem::move(tmpFile, filepath)) {
        return false;
    }
    return true;
}

bool FileUtil::atomicCopy(const string &src, const string &file) {
    string tempFilePath = file + TMP_SUFFIX;

    ErrorCode ec = FileSystem::isExist(tempFilePath);
    if (EC_TRUE == ec) {
        BS_LOG(WARN, "temp file[%s] already exist, remove it.",
               tempFilePath.c_str());
        if (EC_OK != FileSystem::remove(tempFilePath)) {
            BS_LOG(ERROR, "Remove temp file[%s] FAILED.",
                   tempFilePath.c_str());
            return false;
        }
    } else if (EC_FALSE != ec) {
        BS_LOG(ERROR, "Unknown Error with temp file[%s].",
               tempFilePath.c_str());
        return false;
    }
    if (FileSystem::copy(src, tempFilePath) != EC_OK) {
        BS_LOG(ERROR, "Copy file:[%s] to [%s] FAILED.",
               src.c_str(), tempFilePath.c_str());
        return false;
    }
    if (FileSystem::rename(tempFilePath, file) != EC_OK) {
        BS_LOG(ERROR, "Rename file:[%s] to [%s] FAILED.",
               tempFilePath.c_str(), file.c_str());
        return false;
    }
    return true;
}

}
}
