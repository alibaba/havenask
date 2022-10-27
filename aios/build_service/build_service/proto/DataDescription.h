#ifndef ISEARCH_BS_DATADESCRIPTION_H
#define ISEARCH_BS_DATADESCRIPTION_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/proto/ParserConfig.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace proto {

class DataDescription : public  autil::legacy::Jsonizable
{
public:
    DataDescription();
    ~DataDescription();
public:
    DataDescription(const DataDescription &other)
        : parserConfigs(other.parserConfigs)
        , kvMap(other.kvMap)
    {
        
    }
    DataDescription& operator=(const DataDescription &other) {
        parserConfigs = other.parserConfigs;
        kvMap = other.kvMap;
        return *this;
    }
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    KeyValueMap::mapped_type& operator[] (const std::string& key) {
        return kvMap[key];
    }

    KeyValueMap::iterator find(const std::string& key) {
        return kvMap.find(key);
    }

    KeyValueMap::const_iterator find(const std::string& key) const {
        return kvMap.find(key);
    }

    KeyValueMap::mapped_type at(const std::string& key) {
        return kvMap.at(key);
    }

    KeyValueMap::mapped_type at(const std::string& key) const {
        return kvMap.at(key);
    }    

    KeyValueMap::iterator begin() {
        return kvMap.begin();
    }

    KeyValueMap::const_iterator begin() const {
        return kvMap.begin();
    }    

    KeyValueMap::iterator end() {
        return kvMap.end();
    }

    KeyValueMap::const_iterator end() const {
        return kvMap.end();
    }

    bool operator == (const DataDescription& other) const;

    size_t getParserConfigCount() const {
        return parserConfigs.size();
    }

    const ParserConfig& getParserConfig(size_t idx) const {
        return parserConfigs[idx];
    }
        
public:
    std::vector<ParserConfig> parserConfigs;
    KeyValueMap kvMap;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DataDescription);

}
}

#endif //ISEARCH_BS_DATADESCRIPTION_H
