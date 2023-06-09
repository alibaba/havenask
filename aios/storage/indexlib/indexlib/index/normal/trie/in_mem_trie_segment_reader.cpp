/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/index/normal/trie/in_mem_trie_segment_reader.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, InMemTrieSegmentReader);

InMemTrieSegmentReader::InMemTrieSegmentReader(ReadWriteLock* dataLock, KVMap* kvMap)
    : mDataLock(dataLock)
    , mKVMap(kvMap)
{
}

InMemTrieSegmentReader::~InMemTrieSegmentReader()
{
    mDataLock = NULL;
    mKVMap = NULL;
}
}} // namespace indexlib::index
