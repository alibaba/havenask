#ifndef __INDEXLIB_RAW_KEYMAP_H
#define __INDEXLIB_RAW_KEYMAP_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <tr1/unordered_map>
#include <autil/ConstString.h>

IE_NAMESPACE_BEGIN(document);

class KeyMap
{
private:
    struct StringHasher {
        // get first 2 and last 6 characters as hash value.
        // keep a balance between speed and uniqueness.
        size_t operator()(const autil::ConstString& str) const {
            size_t hashValue = 0;
            char *hashValueBuf = (char*)&hashValue;
            size_t copySize;
            if ( str.size() > 6 ) {
                hashValueBuf[6] = str.data()[0];
                hashValueBuf[7] = str.data()[1];
                copySize = 6;
            } else {
                copySize = str.size();
            }
            const char *strBuf = str.data() + str.size() - copySize;
            for (size_t i = 0; i < copySize; i++) {
                hashValueBuf[i] = strBuf[i];    
            }
            return hashValue;
        }
    };

    typedef std::tr1::unordered_map<autil::ConstString, size_t, StringHasher> HashMap;
    typedef std::vector<autil::ConstString> KeyFields;

public:
    KeyMap(size_t keyInitSize = 1000);
    virtual ~KeyMap();
private:
    KeyMap(const KeyMap &);
    KeyMap& operator=(const KeyMap &);
public:
    KeyMap *clone() const;
    void merge(const KeyMap &);
    size_t findOrInsert(const autil::ConstString&);
    size_t find(const autil::ConstString&) const;
    size_t insert(const autil::ConstString&);
    const KeyFields& getKeyFields() const;
    virtual size_t size() const;
public:
    static const size_t INVALID_INDEX;

private:
    HashMap _hashMap;
    KeyFields _keyFields;
    autil::mem_pool::Pool *_pool;
    size_t _keyInitSize;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KeyMap);
///////////////////////////////////////////////////////

inline size_t KeyMap::findOrInsert(const autil::ConstString &key)
{
    HashMap::const_iterator it = _hashMap.find(key);
    if (it != _hashMap.end()) {
        return it->second;
    }
    return insert(key);
}

inline size_t KeyMap::find(const autil::ConstString &key) const
{
    HashMap::const_iterator it = _hashMap.find(key);
    if (it != _hashMap.end()) {
        return it->second;
    }
    return INVALID_INDEX;
}

inline const KeyMap::KeyFields &KeyMap::getKeyFields() const
{
    return _keyFields;
}

inline size_t KeyMap::size() const 
{
    return _keyFields.size();
}

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_RAW_KEYMAP_H
