#ifndef __INDEXLIB_IN_MEM_SEGMENT_RETRIEVER_H
#define __INDEXLIB_IN_MEM_SEGMENT_RETRIEVER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/misc/exception.h"

IE_NAMESPACE_BEGIN(index);

class InMemSegmentRetriever : public IndexSegmentRetriever
{
public:
    InMemSegmentRetriever(const util::KeyValueMap& parameters)
        : IndexSegmentRetriever(parameters)
    {}
    
    ~InMemSegmentRetriever() {}
    
public:
    bool Open(const file_system::DirectoryPtr& indexDir) override
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "not support call Open interface!");
        return false;
    }
    
    size_t EstimateLoadMemoryUse(const file_system::DirectoryPtr& indexDir) const override
    {
        INDEXLIB_FATAL_ERROR(UnSupported, "not support call EstimateLoadMemoryUse interface!");
        return 0;
    }

    // TODO: user should override Search() interface below
    /*
      MatchInfo Search(const std::string& query, autil::mem_pool::Pool* sessionPool) override;
    */
    
private:
    IE_LOG_DECLARE();
};


DEFINE_SHARED_PTR(InMemSegmentRetriever);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_SEGMENT_RETRIEVER_H
