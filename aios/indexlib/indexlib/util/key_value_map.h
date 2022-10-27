#ifndef __INDEXLIB_KEY_VALUE_MAP_H
#define __INDEXLIB_KEY_VALUE_MAP_H

#include <map>
#include <string>

IE_NAMESPACE_BEGIN(util);

typedef std::map<std::string, std::string> KeyValueMap;
static const std::string EMPTY_STRING;

inline const std::string& GetValueFromKeyValueMap(const KeyValueMap& keyValueMap,
        const std::string &key, const std::string &defaultValue = EMPTY_STRING)
{
    KeyValueMap::const_iterator iter = keyValueMap.find(key);
    if (iter != keyValueMap.end()) {
        return iter->second;
    }
    return defaultValue;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_KEY_VALUE_MAP_H
