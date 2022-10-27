#ifndef ISEARCH_GENERATIONMETA_H
#define ISEARCH_GENERATIONMETA_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(config);

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
private:
    std::map<std::string, std::string> _meta;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(GenerationMeta);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_GENERATIONMETA_H
