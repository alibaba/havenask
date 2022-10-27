#ifndef __INDEXLIB_TRIE_INDEX_WRITER_H
#define __INDEXLIB_TRIE_INDEX_WRITER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/primarykey/primary_key_index_writer.h"
#include "indexlib/util/simple_pool.h"
#include "indexlib/index/normal/trie/double_array_trie.h"

IE_NAMESPACE_BEGIN(index);

class TrieIndexWriter : public PrimaryKeyIndexWriter
{
private:
    typedef DoubleArrayTrie::KVPairVector KVPairVector;
    typedef std::map<autil::ConstString, docid_t, std::less<autil::ConstString>,
                     autil::mem_pool::pool_allocator<
                         std::pair<const autil::ConstString, docid_t> > > KVMap;
public:
    TrieIndexWriter();
    ~TrieIndexWriter() {}
private:
    TrieIndexWriter(const TrieIndexWriter &);
    TrieIndexWriter& operator=(const TrieIndexWriter &);

public:
    size_t EstimateInitMemUse(const config::IndexConfigPtr& indexConfig,
                              const index_base::PartitionSegmentIteratorPtr& segIter =
                              index_base::PartitionSegmentIteratorPtr()) override
    {
        return 0;
    }
    void EndDocument(const document::IndexDocument& indexDocument) override;
    void EndSegment() override {}
    void Dump(const file_system::DirectoryPtr& directory,
              autil::mem_pool::PoolBase* dumpPool) override;
    index::IndexSegmentReaderPtr CreateInMemReader() override;
    void GetDumpEstimateFactor(std::priority_queue<double>& factors,
                               std::priority_queue<size_t>& minMemUses) const override;
    bool CheckPrimaryKeyStr(const std::string& str) const override
    { return true; }

private:
    uint32_t GetDistinctTermCount() const override;
    void UpdateBuildResourceMetrics() override;
    
    void DumpTrieIndex(const KVPairVector& sortedVector,
                       const file_system::DirectoryPtr& indexDirectory,
                       autil::mem_pool::PoolBase* dumpPool);
    void DumpRawData(const KVPairVector& sortedVector,
                     const file_system::DirectoryPtr& indexDirectory);

private:
    //TODO: no lock
    mutable autil::ReadWriteLock mDataLock;
    KVMap mKVMap;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRIE_INDEX_WRITER_H
