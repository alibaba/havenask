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
#ifndef __INDEXLIB_TRIE_INDEX_SEGMENT_READER_H
#define __INDEXLIB_TRIE_INDEX_SEGMENT_READER_H

#include <memory>

#include "autil/ConstString.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/index/normal/trie/double_array_trie.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class TrieIndexSegmentReader
{
public:
    typedef std::pair<autil::StringView, docid_t> KVPair;
    typedef std::vector<KVPair, autil::mem_pool::pool_allocator<KVPair>> KVPairVector;

public:
    TrieIndexSegmentReader() : mData(NULL) {}
    ~TrieIndexSegmentReader() {}

public:
    void Open(const config::IndexConfigPtr& indexConfig, const index_base::SegmentData& segmentData);
    docid_t Lookup(const autil::StringView& pkStr) const { return DoubleArrayTrie::Match(mData, pkStr); }

    size_t PrefixSearch(const autil::StringView& key, KVPairVector& results, int32_t maxMatches) const
    {
        return DoubleArrayTrie::PrefixSearch(mData, key, results, maxMatches);
    }

private:
    file_system::DirectoryPtr GetDirectory(const file_system::DirectoryPtr& segmentDirectory,
                                           const config::IndexConfigPtr& indexConfig);

private:
    file_system::FileReaderPtr mDataFile;
    char* mData;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexSegmentReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_TRIE_INDEX_SEGMENT_READER_H
