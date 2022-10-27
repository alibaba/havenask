#ifndef ISEARCH_BS_GENERATIONMETA_H
#define ISEARCH_BS_GENERATIONMETA_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace config {

class GenerationMeta
{
public:
    GenerationMeta();
    ~GenerationMeta();
public:
    static const std::string FILE_NAME;
public:
    bool loadFromFile(const std::string &rootDir);

    const std::string &getValue(const std::string &key) const {
        std::map<std::string, std::string>::const_iterator it = _meta.find(key);
        if (it == _meta.end()) {
            static std::string empty_string;
            return empty_string;
        }
        return it->second;
    }

    bool operator==(const GenerationMeta &other) const;
    bool operator!=(const GenerationMeta &other) const {
        return !(*this == other);
    }
private:
    std::map<std::string, std::string> _meta;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GenerationMeta);

}
}

#endif //ISEARCH_BS_GENERATIONMETA_H
