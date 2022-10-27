#ifndef __INDEXLIB_DIRECTORY_CHECKER_H
#define __INDEXLIB_DIRECTORY_CHECKER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(test);

class DirectoryChecker
{
public:
    DirectoryChecker();
    ~DirectoryChecker();
    
public:
    typedef std::vector<std::pair<uint64_t, std::string>> FileHashes;
    
    static FileHashes GetAllFileHashes(
        const std::string& path, const std::vector<std::string>& excludeFilesPrefix);
    static bool CheckEqual(const FileHashes& expectHashes, const FileHashes& actualHashes);
    static void PrintDifference(const FileHashes& expectHashes, const FileHashes& actualHashes);

private:
    static bool FileNameMatchAnyPrefix(
        const std::string& filePath, const std::vector<std::string>& prefixes);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DirectoryChecker);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_DIRECTORY_CHECKER_H
