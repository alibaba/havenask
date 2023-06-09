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
#ifndef __INDEXLIB_BUILDING_TRIE_INDEX_READER_H
#define __INDEXLIB_BUILDING_TRIE_INDEX_READER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/trie/in_mem_trie_segment_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class BuildingTrieIndexReader
{
public:
    BuildingTrieIndexReader() {}
    ~BuildingTrieIndexReader() {}

public:
    void AddSegmentReader(docid_t baseDocId, const InMemTrieSegmentReaderPtr& inMemSegReader)
    {
        if (inMemSegReader) {
            mInnerSegReaders.push_back(std::make_pair(baseDocId, inMemSegReader));
        }
    }

    docid_t Lookup(const autil::StringView& pkStr) const
    {
        typename SegmentReaders::const_reverse_iterator iter = mInnerSegReaders.rbegin();
        for (; iter != mInnerSegReaders.rend(); ++iter) {
            docid_t docId = iter->second->Lookup(pkStr);
            if (docId == INVALID_DOCID) {
                continue;
            }
            return iter->first + docId;
        }
        return INVALID_DOCID;
    }

private:
    typedef std::pair<docid_t, InMemTrieSegmentReaderPtr> SegmentReaderItem;
    typedef std::vector<SegmentReaderItem> SegmentReaders;
    SegmentReaders mInnerSegReaders;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingTrieIndexReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_BUILDING_TRIE_INDEX_READER_H
