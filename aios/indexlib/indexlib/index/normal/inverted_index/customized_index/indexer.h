#ifndef __INDEXLIB_INDEX_INDEXER_H
#define __INDEXLIB_INDEX_INDEXER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/index_raw_field.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"
#include "indexlib/index/normal/inverted_index/customized_index/in_mem_segment_retriever.h"
#include "indexlib/util/simple_pool.h"
#include "indexlib/util/mmap_allocator.h"
#include <autil/mem_pool/Pool.h>

IE_NAMESPACE_BEGIN(index);

class Indexer
{
public:
    Indexer(const util::KeyValueMap& parameters);
    virtual ~Indexer();

public:
    // this section provides default implementation
    virtual uint32_t GetDistinctTermCount() const;

    virtual size_t EstimateInitMemoryUse(size_t lastSegmentDistinctTermCount) const;

    // estimate current memory use
    virtual int64_t EstimateCurrentMemoryUse() const;

    // estimate memory allocated by pool in Dumping
    virtual int64_t EstimateExpandMemoryUseInDump() const;
    
public:
    bool Init(const IndexerResourcePtr& resource);
    virtual bool DoInit(const IndexerResourcePtr& resource); 
    // for build
    virtual bool Build(const std::vector<const document::Field*>& fieldVec,
                       docid_t docId) = 0; 
    // for build
    virtual bool Dump(const file_system::DirectoryPtr& dir) = 0;
    // estimate temporary memory allocated in Dumping
    virtual int64_t EstimateTempMemoryUseInDump() const = 0;
    // estimate dumped file size
    virtual int64_t EstimateDumpFileSize() const = 0;

    // create InMemSegmentRetriever for building segment
    virtual InMemSegmentRetrieverPtr CreateInMemSegmentRetriever() const
    { return InMemSegmentRetrieverPtr(); }

protected:
    bool GetFieldName(fieldid_t fieldId, std::string& fieldName);
    
protected:
    util::KeyValueMap mParameters;
    config::FieldSchemaPtr mFieldSchema;
    util::MMapAllocator mAllocator;
    autil::mem_pool::Pool mPool; // for allocating memory
    util::SimplePool mSimplePool;    // for aggregating total memory used in STL containers
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Indexer);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_INDEXER_H
