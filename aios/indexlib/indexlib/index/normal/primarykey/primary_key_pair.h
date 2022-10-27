#ifndef __INDEXLIB_PRIMARY_KEY_PAIR_H
#define __INDEXLIB_PRIMARY_KEY_PAIR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

template <typename KeyType>
struct PKPair {
    KeyType key;
    docid_t docid;
    inline bool operator< (const PKPair &rhs) const {
        return key < rhs.key; 
    }
    inline bool operator< (const KeyType &rhs) const {
        return key < rhs; 
    }
    inline bool operator== (const PKPair &rhs) const {
        return key == rhs.key; 
    }
    inline bool operator== (const KeyType &rhs) const {
        return key == rhs; 
    }
};

// The packing size used for class template instantiation is set 
// by the #pragma pack directive which is in effect at the point
// of template instantiation (whether implicit or explicit), 
// rather then by the #pragma pack directive which was in effect 
// at the point of class template definition.

// so we need to explicit the template instantiation here to ensure
// that the packing size of PKPair is same as we expected

#pragma pack(push)
#pragma pack(4)
template <>
struct PKPair<autil::uint128_t> {
    autil::uint128_t key;
    docid_t docid;
    inline bool operator< (const PKPair &rhs) const {
        return key < rhs.key; 
    }
    inline bool operator< (const autil::uint128_t &rhs) const {
        return key < rhs; 
    }
    inline bool operator== (const PKPair &rhs) const {
        return key == rhs.key; 
    }
    inline bool operator== (const autil::uint128_t &rhs) const {
        return key == rhs; 
    }
};

template <>
struct PKPair<uint64_t> {
    uint64_t key;
    docid_t docid;
    inline bool operator< (const PKPair &rhs) const {
        return key < rhs.key; 
    }
    inline bool operator< (const uint64_t &rhs) const {
        return key < rhs; 
    }
    inline bool operator== (const PKPair &rhs) const {
        return key == rhs.key; 
    }
    inline bool operator== (const uint64_t &rhs) const {
        return key == rhs; 
    }
};
#pragma pack(pop)

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_PAIR_H
