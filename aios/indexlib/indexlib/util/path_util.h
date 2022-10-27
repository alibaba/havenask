#ifndef __INDEXLIB_PATH_UTIL_H
#define __INDEXLIB_PATH_UTIL_H

#include "indexlib/indexlib.h"
#include <tr1/memory>

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class PathUtil
{
private:
    static const std::string CONFIG_SEPARATOR;

public:
    PathUtil();
    ~PathUtil();

public:
    static std::string GetParentDirPath(const std::string& path);
    static std::string GetParentDirName(const std::string& path);
    static std::string GetFileName(const std::string& path);
    static std::string JoinPath(const std::string& path, const std::string& name);
    static std::string JoinPath(
        const std::string& basePath, const std::string& path, const std::string& name);
    static std::string NormalizePath(const std::string& path);
    static void TrimLastDelim(std::string& dirPath);

    static bool IsInRootPath(const std::string& normPath, const std::string& normRootPath);

    static bool GetRelativePath(
        const std::string& parentPath, const std::string& path, std::string& relativePath);

    static std::string GetRelativePath(const std::string& absPath);

    static bool IsInDfs(const std::string& path);

    static bool SupportMmap(const std::string& path);

    static std::string AddFsConfig(const std::string& srcPath, const std::string& configStr);
    static std::string SetFsConfig(const std::string& srcPath, const std::string& configStr);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PathUtil);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_PATH_UTIL_H
