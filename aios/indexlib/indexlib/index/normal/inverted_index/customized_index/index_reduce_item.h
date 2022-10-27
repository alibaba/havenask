#ifndef __INDEXLIB_INDEX_REDUCE_ITEM_H
#define __INDEXLIB_INDEX_REDUCE_ITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"

IE_NAMESPACE_BEGIN(index);

class IndexReduceItem
{
public:
    IndexReduceItem();
    virtual ~IndexReduceItem() = 0;
public:
    virtual bool LoadIndex(const file_system::DirectoryPtr& indexDir) = 0;
    virtual bool UpdateDocId(const DocIdMap& docIdMap) = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexReduceItem);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_REDUCE_ITEM_H
