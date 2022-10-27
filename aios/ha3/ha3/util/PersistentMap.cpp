#include <ha3/util/PersistentMap.h>
#include <fslib/fslib.h>
#include <suez/turing/common/FileUtil.h>

using namespace suez::turing;

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, PersistentMap);

const char KEY_VALUE_SEPEARTOR_SIGN = ':';
const char KV_PAIR_SEPRATOR_SIGN = '|';
const char SEMANTIC_CONVERT_SIGN = '\\';
PersistentMap::PersistentMap() { 
}

PersistentMap::~PersistentMap() { 
}

bool PersistentMap::init(const std::string &fslibURI) {
    if (fslibURI.empty() 
        || *(fslibURI.rbegin()) == '/') 
    {
        HA3_LOG(ERROR, "Invalid path:[%s]", fslibURI.c_str());
        return false;
    }
    
    fslib::ErrorCode ec = fslib::fs::FileSystem::isExist(fslibURI) ;
    if (fslib::EC_TRUE == ec) 
    {
        fslib::ErrorCode ec1 = fslib::fs::FileSystem::isFile(fslibURI);
        if (fslib::EC_FALSE == ec1) {
            HA3_LOG(ERROR, "path [%s] is not a file", fslibURI.c_str());
            return false;
        } else if (fslib::EC_TRUE != ec1) {
            HA3_LOG(ERROR, "Failed to check path[%s], error code[%d]",
                    fslibURI.c_str(), ec1);
            return false;
        }
    } else if (fslib::EC_FALSE != ec) {
        HA3_LOG(ERROR, "Failed to check path[%s], error code[%d]",
                fslibURI.c_str(), ec);
        return false;
    }
    _fslibURI = fslibURI;
    return true;
}

bool PersistentMap::download() {
    std::string content; 
    if (!FileUtil::readFromFslibFile(_fslibURI, content)) {
        HA3_LOG(ERROR, "Failed read content from [%s]", _fslibURI.c_str());
        return false;
    }
    _map.clear();
    if (!content.empty()) {
        return deserialize(content);
    }
    return true;
}

bool PersistentMap::upload() {
    std::string content = serialize();
    return FileUtil::writeToFile(_fslibURI, content);
}

bool PersistentMap::find(const std::string &key, std::string &value) const {
    MapType::const_iterator it = _map.find(key);
    if (it != _map.end()) {
        value = it->second;
        return true;
    }
    return false;
}

std::string PersistentMap::findWithDefaultValue(const std::string &key, const std::string &defValue) const {
    MapType::const_iterator it = _map.find(key);
    if (it != _map.end()) {
        return it->second;
    }
    return defValue;
}

void PersistentMap::set(const std::string &key, const std::string &value) {
    _map[key] = value;
}
void PersistentMap::remove(const std::string &key) {
    MapType::iterator it = _map.find(key);
    if (it != _map.end()) {
        _map.erase(it);
    }
}
std::string& PersistentMap::operator [](const std::string &key) {
    return _map[key];
}

const std::string PersistentMap::operator [](const std::string &key) const {
    return findWithDefaultValue(key);
}

size_t PersistentMap::size() const {
    return _map.size();
}

std::string PersistentMap::serialize() const {
    std::string serializedStr;
    for ( MapType::const_iterator it = _map.begin(); 
          it != _map.end(); ++it) 
    {
        std::string keyStr;
        std::string valueStr;
        encodeString(it->first, keyStr);
        encodeString(it->second, valueStr);
        if (!serializedStr.empty()) {
            serializedStr += "|";
        }
        serializedStr += keyStr;
        serializedStr += ":";
        serializedStr += valueStr;
    }
    return serializedStr;
}

bool PersistentMap::deserialize(const std::string &serializedStr) {
    _map.clear();
    size_t startPos = 0;
    size_t endPos = 0;
    std::string key;
    std::string value;
    std::string pairString;
    while( std::string::npos != 
          (endPos = getNextSeperatorPos(serializedStr, startPos, KV_PAIR_SEPRATOR_SIGN )))
    {
        pairString = serializedStr.substr(startPos, endPos-startPos);
        if (!extractKVPair(pairString, key, value)) {
            HA3_LOG(ERROR, "Failed to extract kv value from [%s]", pairString.c_str());
            return false;
        }
        _map[key] = value;
        startPos = endPos+1;
    }
    pairString = serializedStr.substr(startPos);
    if (!extractKVPair(pairString, key, value)) {
        HA3_LOG(ERROR, "Failed to extract kv value from [%s]", pairString.c_str());
        return false;
    }
    _map[key] = value;
    return true;
}

bool PersistentMap::encodeString(const std::string &in, std::string &out) const
{
    for (std::string::const_iterator it = in.begin();
         it != in.end(); ++it)
    {
        if (KEY_VALUE_SEPEARTOR_SIGN == (*it)
            || KV_PAIR_SEPRATOR_SIGN == (*it)
            || SEMANTIC_CONVERT_SIGN == (*it) ) 
        {
            out.push_back(SEMANTIC_CONVERT_SIGN);
        }
        out.push_back(*it);
    }
    return true;
}
bool PersistentMap::decodeString(const std::string &in, std::string &out) const
{
    bool flag = false;
    for (std::string::const_iterator it = in.begin();
         it != in.end(); ++it)
    {
        if ( false == flag 
            && SEMANTIC_CONVERT_SIGN == (*it) ) 
        {
            flag = true;
            continue;
        }
        flag = false;
        out.push_back(*it);
    }
    return true;
}

bool PersistentMap::extractKVPair(const std::string &content, 
                                  std::string &key, std::string &value) const
{
    size_t endPos = getNextSeperatorPos(content, 0, KEY_VALUE_SEPEARTOR_SIGN);
    if (std::string::npos == endPos ||
        std::string::npos != getNextSeperatorPos(content, endPos+1, KEY_VALUE_SEPEARTOR_SIGN))
    {
        HA3_LOG(ERROR, "Invalid kvPair [%s]", content.c_str());
        return false;
    }
    key = "";
    value = "";
    std::string keyStr = content.substr(0, endPos);
    std::string valueStr = content.substr(endPos+1);
    return (decodeString(keyStr, key) && decodeString(valueStr, value));
}

size_t PersistentMap::getNextSeperatorPos(const std::string &content,
        size_t startPos, char seperatorSign) const
{
    if (startPos == std::string::npos) {
        return startPos;
    }

    size_t pairPos = content.find_first_of(seperatorSign, startPos);
    while (pairPos != std::string::npos && pairPos != 0) {
       size_t endPos = content.find_last_not_of(SEMANTIC_CONVERT_SIGN, pairPos-1);
       size_t slashCount = pairPos;
       if (std::string::npos != endPos ) {
           slashCount = pairPos - 1 - endPos;
       } 
       if( (slashCount & 1 ) == 0) {
           break;
       } else {
           pairPos = content.find_first_of(seperatorSign, pairPos+1);
       }
    }

    return pairPos;
}



END_HA3_NAMESPACE(util);

