#ifndef ISEARCH_CONSISTENTHASH_H
#define ISEARCH_CONSISTENTHASH_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>

BEGIN_HA3_NAMESPACE(util);

template<typename T>
class ConsistentHash
{
public:
    typedef typename std::vector<std::pair<uint64_t, T> >::const_iterator Iterator; 
private:
    static const uint32_t DEFAULT_VIRTUAL_NODE_COUNT = 10;
public:
    ConsistentHash(uint32_t virtualNodeCount = DEFAULT_VIRTUAL_NODE_COUNT)
        : _virtualNodeCount(virtualNodeCount)
    {
    }
    
    ~ConsistentHash() {}
private:
    ConsistentHash& operator=(const ConsistentHash &other);    
    ConsistentHash(const ConsistentHash &);
public:
    void add(uint64_t key, const T& node, uint32_t factor = 1);
    
    Iterator get(uint64_t key);
    
    // you should call construct before use!
    void construct() {
        sort(_hashCircle.begin(), _hashCircle.end());
    }
    
    Iterator begin() {
        return _hashCircle.begin();
    }
    Iterator end() {
        return _hashCircle.end();
    }
public:
    // for test
    size_t size() {
        return _hashCircle.size();
    }
private:
    uint32_t _virtualNodeCount;
    std::vector<std::pair<uint64_t, T> > _hashCircle;
private:
    HA3_LOG_DECLARE();
};

template<typename T>
void ConsistentHash<T>::add(uint64_t key, const T& node, uint32_t factor) {
    size_t virtualNodeCount = _virtualNodeCount * factor;
    uint64_t virtualKey = key;
    for (size_t i = 0; i < virtualNodeCount; ++i) {
        _hashCircle.push_back(std::make_pair(virtualKey, node));
        virtualKey = virtualKey * 1103515245 + 12345;
    }
}

template<typename T>
typename ConsistentHash<T>::Iterator ConsistentHash<T>::get(uint64_t key) {
    if (_hashCircle.empty()) {
        return _hashCircle.end();
    }
    std::pair<uint64_t, T> findKey = std::make_pair(key, T());
    Iterator it = lower_bound(_hashCircle.begin(), _hashCircle.end(), findKey);
    if (it == _hashCircle.end()) {
        it = _hashCircle.begin();
    }
    return it;
}

END_HA3_NAMESPACE(util);

#endif //ISEARCH_CONSISTENTHASH_H
