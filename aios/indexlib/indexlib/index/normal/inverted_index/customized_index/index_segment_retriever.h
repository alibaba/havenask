#ifndef __INDEXLIB_INDEX_INDEX_SEGMENT_RETRIEVER_H
#define __INDEXLIB_INDEX_INDEX_SEGMENT_RETRIEVER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/key_value_map.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib/file_system/directory.h"
#include <autil/mem_pool/Pool.h>

IE_NAMESPACE_BEGIN(index);

class IndexSegmentRetriever
{
public:
    IndexSegmentRetriever(const util::KeyValueMap& parameters)
        : mParameters(parameters)
    {};
    
    virtual ~IndexSegmentRetriever() {}
public:
    virtual bool Open(const file_system::DirectoryPtr& indexDir) = 0;
    virtual MatchInfo Search(
            const std::string& query, autil::mem_pool::Pool* sessionPool) = 0;
    virtual size_t EstimateLoadMemoryUse(const file_system::DirectoryPtr& indexDir) const = 0;
    
protected:
    util::KeyValueMap mParameters; 
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexSegmentRetriever);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_SEGMENT_RETRIVER_H
