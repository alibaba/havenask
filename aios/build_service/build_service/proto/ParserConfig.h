#ifndef ISEARCH_BS_PARSERCONFIG_H
#define ISEARCH_BS_PARSERCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace proto {

class ParserConfig : public  autil::legacy::Jsonizable
{
public:
    ParserConfig() {}
    ~ParserConfig() {}
public:
    ParserConfig(const ParserConfig &other)
        : type(other.type)
        , parameters(other.parameters)
    {}
    
    ParserConfig& operator=(const ParserConfig &other) {
        type = other.type;
        parameters = other.parameters;
        return *this;
    }
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("type", type, type);
        json.Jsonize("parameters", parameters, parameters); 
    }
    bool operator == (const ParserConfig& other) const {
        return type == other.type && (parameters == other.parameters);
    }
    bool operator != (const ParserConfig& other) const {
        return ! (operator == (other));
    }    

public:
    std::string type;
    KeyValueMap parameters;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ParserConfig);

}
}

#endif //ISEARCH_BS_PARSERCONFIG_H
