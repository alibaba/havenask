#ifndef __INDEXLIB_INDEX_WIRTER_H
#define __INDEXLIB_INDEX_WIRTER_H

#include <tr1/memory>
#include <string>
#include <queue>
#include <assert.h>
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include <autil/StringUtil.h>
#include <autil/HashAlgorithm.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/simple_pool.h"

DECLARE_REFERENCE_CLASS(document, Field);
DECLARE_REFERENCE_CLASS(document, IndexDocument);
DECLARE_REFERENCE_CLASS(index, IndexSegmentReader);
DECLARE_REFERENCE_CLASS(index_base, SegmentMetrics);
DECLARE_REFERENCE_CLASS(index_base, PartitionSegmentIterator);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, MMapAllocator);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

IE_NAMESPACE_BEGIN(index);

class IndexWriter
{
public:
    IndexWriter()
        : mBuildResourceMetricsNode(NULL)
    {}
    
    virtual ~IndexWriter(){}

public:
    virtual void Init(const config::IndexConfigPtr& indexConfig,
                      util::BuildResourceMetrics* buildResourceMetrics,
                      const index_base::PartitionSegmentIteratorPtr& segIter =
                      index_base::PartitionSegmentIteratorPtr());
    
    virtual size_t EstimateInitMemUse(
            const config::IndexConfigPtr& indexConfig,
            const index_base::PartitionSegmentIteratorPtr& segIter =
            index_base::PartitionSegmentIteratorPtr()) = 0;

    virtual void AddField(const document::Field* field) = 0;
    virtual void EndDocument(const document::IndexDocument& indexDocument) = 0;
    virtual void EndSegment() = 0;

    virtual void Dump(const file_system::DirectoryPtr& dir,
                      autil::mem_pool::PoolBase* dumpPool) = 0;

    virtual const config::IndexConfigPtr& GetIndexConfig() const
    {
        return mIndexConfig;
    }

    virtual index::IndexSegmentReaderPtr CreateInMemReader() = 0;

    // for test
    virtual uint64_t GetNormalTermDfSum() const 
    { assert(false); return 0; }
    virtual void GetDumpEstimateFactor(std::priority_queue<double>& factors,
            std::priority_queue<size_t>& minMemUses) const {}
    virtual void FillDistinctTermCount(index_base::SegmentMetricsPtr mSegmentMetrics);
    static dictkey_t GetRetrievalHashKey(
            bool isNumberIndex, dictkey_t termHashKey)
    {
        if (!isNumberIndex)
        {
            return termHashKey;
        }

        // number index termHashKey may cause hash confict, rehash
        return autil::HashAlgorithm::hashString64(
                (const char*)&termHashKey, sizeof(dictkey_t));
    }
private:
    virtual uint32_t GetDistinctTermCount() const = 0;

protected:
    typedef std::tr1::shared_ptr<autil::mem_pool::Pool> PoolPtr;
    
protected:
    virtual void InitMemoryPool();
    virtual void UpdateBuildResourceMetrics() = 0;
    
protected:
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;
    config::IndexConfigPtr mIndexConfig;
    util::MMapAllocatorPtr mAllocator;
    PoolPtr mByteSlicePool;
    util::SimplePool mSimplePool;
    
private:
    IE_LOG_DECLARE();

};

DEFINE_SHARED_PTR(IndexWriter);

IE_NAMESPACE_END(index);

#endif
