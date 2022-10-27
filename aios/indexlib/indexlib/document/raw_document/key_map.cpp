#include <algorithm>
#include "indexlib/document/raw_document/key_map.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, KeyMap);

const size_t KeyMap::INVALID_INDEX = (size_t)-1;

KeyMap::KeyMap(size_t keyInitSize)
    : _hashMap(keyInitSize)
    , _pool(new Pool(1024))
    , _keyInitSize(keyInitSize)
{
}

KeyMap::~KeyMap() {
    _hashMap.clear();
    _keyFields.clear();
    DELETE_AND_SET_NULL(_pool);
}

KeyMap::KeyMap(const KeyMap &other)
    : _hashMap(other._keyInitSize)
    , _pool(new Pool(1024))
    , _keyInitSize(other._keyInitSize)
{
    for (size_t i=0; i< other._keyFields.size(); ++i)
    {
        ConstString key(other._keyFields[i], _pool);
        _hashMap.insert(make_pair(key, i));
        _keyFields.push_back(key);
    }
}

KeyMap* KeyMap::clone() const
{
    return new KeyMap(*this);
}

void KeyMap::merge(const KeyMap &increment)
{
    for (KeyFields::const_iterator it = increment._keyFields.begin();
         it != increment._keyFields.end(); ++it)
    {
        findOrInsert(*it);
    }
}

size_t KeyMap::insert(const ConstString &key)
{
    size_t indexNew = _hashMap.size();
    ConstString newKey = ConstString(key, _pool);
    _keyFields.push_back(newKey);
    _hashMap[newKey] = indexNew;
    assert(indexNew + 1 == _hashMap.size());
    return indexNew;
}

IE_NAMESPACE_END(document);

