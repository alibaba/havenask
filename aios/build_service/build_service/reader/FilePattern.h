#ifndef ISEARCH_BS_FILEPATTERN_H
#define ISEARCH_BS_FILEPATTERN_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/reader/FilePatternBase.h"

namespace build_service {
namespace reader {

class FilePattern : public FilePatternBase
{
public:
    FilePattern(const std::string &dirPath, const std::string &prefix, 
                const std::string &suffix, uint32_t fileCount = 0);
    ~FilePattern();
public:
    /* override */ CollectResults calculateHashIds(const CollectResults &results) const;
public:
    bool operator==(const FilePattern &other) const;
public:
    void setPrefix(const std::string &prefix)  { _prefix = prefix; }
    const std::string& getPrefix() const { return _prefix; }

    void setSuffix(const std::string &suffix) { _suffix = suffix; }
    const std::string& getSuffix() const { return _suffix; }

    void setFileCount(uint32_t count) { _fileCount = count; }
    uint32_t getFileCount() const { return _fileCount; }

    void setRootPath(const std::string &rootPath) { _rootPath = rootPath; }
    const std::string &getRootPath() const { return _rootPath; }

private:
    bool extractFileId(const std::string &fileName, uint32_t &fileId) const;
private:
    std::string _prefix;
    std::string _suffix;
    uint32_t _fileCount;
    std::string _rootPath;
private:
    BS_LOG_DECLARE();
};
BS_TYPEDEF_PTR(FilePattern);

}
}

#endif //ISEARCH_BS_FILEPATTERN_H
