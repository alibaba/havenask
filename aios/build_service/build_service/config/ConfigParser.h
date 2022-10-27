#ifndef ISEARCH_BS_CONFIGPARSER_H
#define ISEARCH_BS_CONFIGPARSER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/json.h>
#include <unordered_map>

namespace build_service {
namespace config {
class ResourceReader;
class ConfigParser
{
public:
    enum ParseResult {
        PR_OK,
        PR_FAIL,
        PR_SECTION_NOT_EXIST
    };

private:
    ConfigParser(const std::string &basePath,
                 const std::unordered_map<std::string, std::string> &fileMap =
                 std::unordered_map<std::string, std::string>());
    ~ConfigParser();
private:
    ConfigParser(const ConfigParser &);
    ConfigParser& operator=(const ConfigParser &);

private:
    bool parse(const std::string &path,
               autil::legacy::json::JsonMap &jsonMap,
               bool isRelativePath = true) const
    {
        if (isRelativePath) {
            return parseRelativePath(path, jsonMap);
        }
        return parseExternalFile(path, jsonMap);
    }

    bool parseRelativePath(const std::string &relativePath,
                           autil::legacy::json::JsonMap &jsonMap) const;

    bool parseExternalFile(const std::string &file,
                           autil::legacy::json::JsonMap &jsonMap) const;

private:
    bool merge(autil::legacy::json::JsonMap &jsonMap, bool isExternal) const;
    bool mergeJsonMap(const autil::legacy::json::JsonMap &src,
                      autil::legacy::json::JsonMap &dst,
                      bool isExternal) const;
    bool mergeJsonArray(autil::legacy::json::JsonArray &dst,
                        bool isExternal) const;
    bool mergeWithInherit(autil::legacy::json::JsonMap &jsonMap,
                          bool isExternal) const;
private:
    std::string _basePath;
    std::unordered_map<std::string, std::string> _fileMap;
    mutable std::vector<std::string> _configFileNames; // for inherit loop detection
private:
    friend class ResourceReader;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_CONFIGPARSER_H
