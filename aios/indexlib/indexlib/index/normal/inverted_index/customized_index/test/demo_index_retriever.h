#ifndef __INDEXLIB_INDEX_DEMO_INDEX_RETRIEVER_H
#define __INDEXLIB_INDEX_DEMO_INDEX_RETRIEVER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_segment_retriever.h"

IE_NAMESPACE_BEGIN(index);

class DemoIndexRetriever : public IndexRetriever
{
public:
    DemoIndexRetriever() {}
    ~DemoIndexRetriever() {}
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoIndexRetriever);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_DEMO_INDEX_RETRIEVER_H
