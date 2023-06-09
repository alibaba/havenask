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
#ifndef __INDEXLIB_TRIE_INDEX_WRITER_H
#define __INDEXLIB_TRIE_INDEX_WRITER_H

#include <memory>

#include "autil/ConstString.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_writer.h"
#include "indexlib/index/normal/trie/double_array_trie.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib { namespace index {

class TrieIndexWriter : public PrimaryKeyWriter
{
private:
    typedef DoubleArrayTrie::KVPairVector KVPairVector;
    typedef std::map<autil::StringView, docid_t, std::less<autil::StringView>,
                     autil::mem_pool::pool_allocator<std::pair<const autil::StringView, docid_t>>>
        KVMap;

public:
    TrieIndexWriter();
    ~TrieIndexWriter() {}

private:
    TrieIndexWriter(const TrieIndexWriter&);
    TrieIndexWriter& operator=(const TrieIndexWriter&);

public:
    size_t EstimateInitMemUse(
        const config::IndexConfigPtr& indexConfig,
        const index_base::PartitionSegmentIteratorPtr& segIter = index_base::PartitionSegmentIteratorPtr()) override
    {
        return 0;
    }
    void EndDocument(const document::IndexDocument& indexDocument) override;
    void EndSegment() override {}
    void Dump(const file_system::DirectoryPtr& directory, autil::mem_pool::PoolBase* dumpPool) override;
    index::IndexSegmentReaderPtr CreateInMemReader() override;
    void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                               std::priority_queue<size_t>& minMemUses) const override;
    bool CheckPrimaryKeyStr(const std::string& str) const override { return true; }

private:
    uint32_t GetDistinctTermCount() const override;
    void UpdateBuildResourceMetrics() override;

    void DumpTrieIndex(const KVPairVector& sortedVector, const file_system::DirectoryPtr& indexDirectory,
                       autil::mem_pool::PoolBase* dumpPool);
    void DumpRawData(const KVPairVector& sortedVector, const file_system::DirectoryPtr& indexDirectory);

private:
    // TODO: no lock
    mutable autil::ReadWriteLock mDataLock;
    util::SimplePool _simplePool;
    KVMap mKVMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexWriter);
}} // namespace indexlib::index

#endif //__INDEXLIB_TRIE_INDEX_WRITER_H
