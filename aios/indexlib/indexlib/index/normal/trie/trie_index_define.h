#ifndef __INDEXLIB_TRIE_INDEX_DEFINE_H
#define __INDEXLIB_TRIE_INDEX_DEFINE_H

#include <algorithm>

#include "indexlib/common_define.h"
#include <limits>

IE_NAMESPACE_BEGIN(index);

const uint8_t TRIE_INDEX_VERSION = 1; // for compitable in the feature
const int32_t TRIE_MAX_MATCHES = std::numeric_limits<int32_t>::max();


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRIE_INDEX_DEFINE_H
