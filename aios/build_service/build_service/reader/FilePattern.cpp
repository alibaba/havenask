#include "build_service/reader/FilePattern.h"
#include "build_service/util/FileUtil.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace build_service::util;
namespace build_service {
namespace reader {

BS_LOG_SETUP(reader, FilePattern);

FilePattern::FilePattern(const string &dirPath, const string &prefix, 
                         const string &suffix, uint32_t fileIdCount)
    : FilePatternBase(dirPath)
    , _prefix(prefix)
    , _suffix(suffix)
    , _fileCount(fileIdCount)
{
}

FilePattern::~FilePattern() {
}

CollectResults FilePattern::calculateHashIds(const CollectResults &fileList) const {
    CollectResults results;
    for (size_t i = 0; i < fileList.size(); ++i) {
        vector<string> dirs = StringUtil::split(fileList[i]._fileName, "/", true);
        string fileName = dirs.back();

        uint32_t fileId;
        if (!extractFileId(fileName, fileId)) {
            continue;
        }
        if (fileId >= _fileCount) {
            continue;
        }

        // match file pattern
        dirs.pop_back();
        string pathDir = StringUtil::toString(dirs, "/");
        string expectedPathDir = FileUtil::joinFilePath(_rootPath, _relativePath);
        dirs = StringUtil::split(expectedPathDir, "/", true);
        expectedPathDir = StringUtil::toString(dirs, "/");
        if (expectedPathDir != pathDir) {
            continue;
        }

        CollectResult now = fileList[i];
        now._rangeId = getRangeId(fileId, _fileCount);
        results.push_back(now);
    }

    return results;
}

bool FilePattern::operator==(const FilePattern &other) const {
    return _relativePath == other._relativePath
        && _prefix == other._prefix
        && _suffix == other._suffix
        && _fileCount == other._fileCount
        && _rootPath == other._rootPath;
}

bool FilePattern::extractFileId(
        const string &fileName, uint32_t &fileId) const
{
    size_t prefixLen = _prefix.length();
    size_t suffixLen = _suffix.length();
    size_t len = fileName.length();
    if (len <= prefixLen + suffixLen) {
        return false;
    }
    if (fileName.substr(0, prefixLen) != _prefix ||
        fileName.substr(len - suffixLen, suffixLen) != _suffix)
    {
        return false;
    }
    string idStr = fileName.substr(prefixLen, len - suffixLen - prefixLen);
    if (!StringUtil::strToUInt32(idStr.c_str(), fileId)) {
        return false;
    }
    return true;
}

}
}
