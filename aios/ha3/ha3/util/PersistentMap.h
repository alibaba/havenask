#ifndef ISEARCH_PERSISTENTMAP_H
#define ISEARCH_PERSISTENTMAP_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/StringUtil.h>

BEGIN_HA3_NAMESPACE(util);

class PersistentMap
{
private:
    typedef std::map<std::string, std::string> MapType;
public:
    PersistentMap();
    ~PersistentMap();
private:
    PersistentMap(const PersistentMap &);
    PersistentMap& operator = (const PersistentMap &);
public:
    bool init(const std::string &fslibURI);
    bool download();
    bool upload();
    bool find(const std::string &key, std::string &value) const;
    std::string findWithDefaultValue(const std::string &key, const std::string &defValue = "") const;
    void set(const std::string &key, const std::string &value);
    void remove(const std::string &key);
    std::string & operator [](const std::string &key);
    const std::string operator [](const std::string &key) const;
    size_t size() const;
public:
    template < typename ValueType >
    ValueType findWithDefaultValue(const std::string &key, const ValueType &defValue) const {
        std::string value;
        if(find(key, value)) {
            return autil::StringUtil::fromString<ValueType>(value);
        }
        else {
            return defValue;
        }
    }
    template < typename ValueType >
    void set(const std::string &key, const ValueType &value) {
        set(key, autil::StringUtil::toString(value));
    }
private:
    std::string serialize() const;
    bool deserialize(const std::string &serializedStr);
    bool encodeString(const std::string &in, std::string &out) const;
    bool decodeString(const std::string &in, std::string &out) const;
    bool extractKVPair(const std::string &content, 
                       std::string &key, std::string &value) const;
    size_t getNextSeperatorPos(const std::string &content, 
                               size_t startPos, char seperatorSign)const;
private:
    MapType _map;
    std::string _fslibURI;
private:
    friend class PersistentMapTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(PersistentMap);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_PERSISTENTMAP_H
