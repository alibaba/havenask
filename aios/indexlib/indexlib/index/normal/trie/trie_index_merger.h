#ifndef __INDEXLIB_TRIE_INDEX_MERGER_H
#define __INDEXLIB_TRIE_INDEX_MERGER_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger_creator.h"
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"
#include "indexlib/index/normal/trie/double_array_trie.h"
#include "indexlib/util/simple_pool.h"

DECLARE_REFERENCE_CLASS(partition, SegmentData);
DECLARE_REFERENCE_CLASS(file_system, FileReader);

IE_NAMESPACE_BEGIN(index);

class TrieIndexMerger : public IndexMerger
{
private:
    typedef DoubleArrayTrie::KVPairVector KVPairVector;
    typedef std::map<autil::ConstString, docid_t> KVMap;

public:
    TrieIndexMerger();
    ~TrieIndexMerger();

public:
    DECLARE_INDEX_MERGER_IDENTIFIER(trie);
    DECLARE_INDEX_MERGER_CREATOR(TrieIndexMerger, it_trie);

public:
    void Merge(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) 
    {
        return InnerMerge(resource, segMergeInfos, outputSegMergeInfos);
    }

    void SortByWeightMerge(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos) 
    {
        return Merge(resource, segMergeInfos, outputSegMergeInfos);
    }
    
    int64_t EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir,
                              const MergerResource& resource, 
                              const index_base::SegmentMergeInfos& segMergeInfos,
                              const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                              bool isSortedMerge) override;

private:
    void InnerMerge(const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos);
    void MergeRawData(
        const index::MergerResource& resource,
        const index_base::SegmentMergeInfos& segMergeInfos,
        std::vector<KVPairVector>& sortedVectors);
    
    void ReadSegment(const index_base::SegmentData& segmentData,
                     const index::MergerResource& resource,
                     std::vector<KVPairVector>& kvMap);

    size_t ReadData(const file_system::FileReaderPtr& fileReader, size_t cursor,
                    size_t length, void* buffer) const;

    OnDiskIndexIteratorCreatorPtr CreateOnDiskIndexIteratorCreator() override
    { return OnDiskIndexIteratorCreatorPtr(); }

private:
    autil::mem_pool::Pool mPool; // TODO: consider
    util::SimplePool mSimplePool;
private:
    friend class TrieIndexMergerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TrieIndexMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRIE_INDEX_MERGER_H
