#pragma once

#include <functional>
#include <map>
#include <vector>

#include "autil/Log.h"

namespace indexlib { namespace file_system {

class DirectoryChecker
{
public:
    typedef std::vector<std::pair<uint64_t, std::string>> FileHashes;
    // {fileName --> changer}, skip file if changer is nullptr
    typedef std::map<std::string, std::function<std::string(const std::string&)>> ContentChanger;
    static FileHashes GetAllFileHashes(const std::string& path, const ContentChanger& contentChanger);
    static bool CheckEqual(const FileHashes& expectHashes, const FileHashes& actualHashes);
    static void PrintDifference(const FileHashes& expectHashes, const FileHashes& actualHashes);

private:
    static bool FileNameMatchAnyPrefix(const std::string& filePath, const std::vector<std::string>& prefixes);

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::file_system
