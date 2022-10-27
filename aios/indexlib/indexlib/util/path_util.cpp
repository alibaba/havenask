#include <fslib/fslib.h>
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, PathUtil);

const string PathUtil::CONFIG_SEPARATOR("?");

PathUtil::PathUtil() 
{
}

PathUtil::~PathUtil() 
{
}

string PathUtil::GetParentDirPath(const string& path)
{
    string::const_reverse_iterator it;
    for (it = path.rbegin(); it != path.rend() && *it == '/'; it++);
    for (; it != path.rend() && *it != '/'; it++);
    for (; it != path.rend() && *it == '/'; it++);
    return path.substr(0, path.rend() - it);
}

string PathUtil::GetParentDirName(const string& path)
{
    string parentDirPath = GetParentDirPath(path);
    string::const_reverse_iterator it;
    for (it = parentDirPath.rbegin(); it != parentDirPath.rend() && *it != '/'; it++);
    return parentDirPath.substr(parentDirPath.rend() - it);
}

string PathUtil::GetFileName(const string& path)
{
    string::const_reverse_iterator it;
    for (it = path.rbegin(); it != path.rend() && *it != '/'; it++);
    return path.substr(path.rend() - it);
}

string PathUtil::JoinPath(const string &path, const string &name)
{
    if (path.empty()) 
    { 
        return name; 
    }

    if (*(path.rbegin()) != '/')
    {
        return path + "/" + name;
    } 

    return path + name;
}

string PathUtil::JoinPath(const std::string &basePath, 
                               const std::string &path, 
                               const std::string &name)
{
    string p = JoinPath(basePath, path);
    return JoinPath(p, name);
}

string PathUtil::NormalizePath(const string& path)
{
#define PU_MAX_PATH_LEN 8192 // 8K
#define CHECK_CURSOR(cursor)                                            \
    if (cursor > PU_MAX_PATH_LEN)                                       \
    {                                                                   \
        INDEXLIB_FATAL_ERROR(UnSupported,                               \
                             "path [%s] length over 8K!", path.c_str()); \
    }                                                                   \

    char pathBuffer[PU_MAX_PATH_LEN] = {0};
    uint32_t cursor = 0;
    size_t start = 0;
    bool hasOneSlash = false;
    size_t idx = path.find("://");
    if (idx != string::npos)
    {
        size_t end = idx + 3;
        for (size_t i = 0; i < end; i++)
        {
            pathBuffer[cursor++] = path[i];
            CHECK_CURSOR(cursor);
        }
        start = end;
        for (; start < path.size(); ++start)
        {
            if (path[start] != '/')
            {
                break;
            }
        }
    }
    else if (!path.empty() && path[0] != '/')
    {
        hasOneSlash = true;
    }
    
    char lastChar = 0;
    for (size_t i = start; i < path.size(); ++i)
    {
        if (lastChar == '/' && path[i] == '/')
        {
            continue;
        }

        if (lastChar == '/')
        {
            hasOneSlash = true;
        }
        lastChar = path[i];
        pathBuffer[cursor++] = lastChar;
        CHECK_CURSOR(cursor);
    }

    if (hasOneSlash && cursor > 0 && pathBuffer[cursor - 1] == '/')
    {
        pathBuffer[--cursor] = '\0';
    }
    return string(pathBuffer);
}

void PathUtil::TrimLastDelim(string& dirPath)
{
    if (dirPath.empty())
    {
        return;
    }
    if (*(dirPath.rbegin()) == '/')
    {
        dirPath.erase(dirPath.size() - 1, 1);    
    }
}

bool PathUtil::IsInRootPath(const string& normPath, 
                            const string& normRootPath)
{
    size_t prefixLen = normRootPath.size();
    if (!normRootPath.empty() && *normRootPath.rbegin() == '/')
    {
        prefixLen--;
    }
    
    if (normPath.size() < normRootPath.size()
        || normPath.compare(0, normRootPath.size(), normRootPath) != 0 
        || (normPath.size() > normRootPath.size() 
            && normPath[prefixLen] != '/'))
    {
        return false;
    }
    return true;
}

bool PathUtil::GetRelativePath(const string& parentPath,
                               const string& path, string& relativePath)
{
    string normalParentPath = parentPath;
    if (!normalParentPath.empty() && *(normalParentPath.rbegin()) != '/')
    {
        normalParentPath += '/';
    }

    if (path.find(normalParentPath) != 0)
    {
        return false;
    }
    relativePath = path.substr(normalParentPath.length());
    return true;
}

string PathUtil::GetRelativePath(const string& absPath)
{
    size_t idx = absPath.find("://");
    if (idx == std::string::npos)
    {
        return string(absPath);
    }
    idx = absPath.find('/', idx + 3);
    return absPath.substr(idx, absPath.length()-idx);
}

bool PathUtil::IsInDfs(const string& path)
{
    FsType fsType = FileSystem::getFsType(path);
    return fsType != FSLIB_FS_LOCAL_FILESYSTEM_NAME;
}

bool PathUtil::SupportMmap(const string& path)
{
    FsType fsType = FileSystem::getFsType(path);
    return fsType == FSLIB_FS_LOCAL_FILESYSTEM_NAME || fsType == "dcache";
}

string PathUtil::AddFsConfig(const string& srcPath, const string& configStr)
{
    size_t found = srcPath.find("://");
    if (found == string::npos) {
        IE_LOG(DEBUG, "path [%s] do not have a scheme", srcPath.c_str());
        return srcPath;
    }

    size_t fsEnd = srcPath.find('/', found + ::strlen("://"));
    if (fsEnd == string::npos) {
        IE_LOG(DEBUG, "path [%s] do not have a valid path", srcPath.c_str());
        return srcPath;
    }
    if (configStr.empty()) {
        return srcPath;
    }
    size_t paramBegin = srcPath.find(CONFIG_SEPARATOR, found);
    bool hasParam = paramBegin != string::npos && paramBegin < fsEnd;
    if (hasParam)
    {
        return srcPath.substr(0, fsEnd) + '&' + configStr
            + srcPath.substr(fsEnd, srcPath.size() - fsEnd);
    }
    return srcPath.substr(0, fsEnd) + CONFIG_SEPARATOR + configStr
        + srcPath.substr(fsEnd, srcPath.size() - fsEnd);
}


string PathUtil::SetFsConfig(const string& srcPath, const string& configStr)
{
    size_t found = srcPath.find("://");
    if (found == string::npos) {
        IE_LOG(DEBUG, "path [%s] do not have a scheme", srcPath.c_str());
        return srcPath;
    }
    size_t fsEnd = srcPath.find('/', found + ::strlen("://"));
    if (fsEnd == string::npos) {
        IE_LOG(DEBUG, "path [%s] do not have a valid path", srcPath.c_str());
        return srcPath;
    }
    if (configStr.empty()) {
        return srcPath;
    }
    return srcPath.substr(0, fsEnd) + CONFIG_SEPARATOR + configStr +
        srcPath.substr(fsEnd, srcPath.size() - fsEnd);
}

IE_NAMESPACE_END(util);

