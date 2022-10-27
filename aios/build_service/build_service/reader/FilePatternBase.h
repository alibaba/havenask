#ifndef ISEARCH_BS_FILEPATTERNBASE_H
#define ISEARCH_BS_FILEPATTERNBASE_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/CollectResult.h"
#include <autil/KsHashFunction.h>
#include <autil/StringUtil.h>

namespace build_service {
namespace reader {

class FilePatternBase;
BS_TYPEDEF_PTR(FilePatternBase);

class FilePatternBase
{
public:
    FilePatternBase(const std::string &dirPath) {
        setDirPath(dirPath);
    }
    virtual ~FilePatternBase()
    {}
public:
    virtual CollectResults calculateHashIds(const CollectResults &results) const = 0;
public:
    void setDirPath(const std::string &dirPath) { _relativePath = normalizeDir(dirPath); }
    const std::string& getDirPath() const { return _relativePath; }
public:
    static bool isPatternConsistent(const FilePatternBasePtr &lft, const FilePatternBasePtr &rhs);
protected:
    static uint16_t getRangeId(uint32_t fileId, uint32_t fileCount) {
        uint32_t fullRange = 65536;
        uint32_t hashid = autil::KsHashFunction::getHashId(fileId, fileCount, fullRange);
        uint16_t rangeId = hashid % 65536;
        return rangeId;
    }
private:
    static std::string normalizeDir(const std::string &path) {
        std::vector<std::string> dirs = autil::StringUtil::split(path, "/", true);
        return autil::StringUtil::toString(dirs, "/");
    }
protected:
    std::string _relativePath;
};

typedef std::vector<FilePatternBasePtr> FilePatterns;

}
}

#endif //ISEARCH_BS_FILEPATTERNBASE_H
