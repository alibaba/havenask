#ifndef __INDEXLIB_INDEX_MERGER_CREATOR_H
#define __INDEXLIB_INDEX_MERGER_CREATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/index/normal/inverted_index/accessor/index_merger.h"

IE_NAMESPACE_BEGIN(index);

#define DECLARE_INDEX_MERGER_CREATOR(classname, indextype)              \
    class Creator : public IndexMergerCreator                           \
    {                                                                   \
    public:                                                             \
        IndexType GetIndexType() const {return indextype;}              \
        IndexMerger* Create() const {return (new classname());}         \
    };

class IndexMergerCreator
{
public:
    IndexMergerCreator(){}
    virtual ~IndexMergerCreator() {}

public:
    virtual IndexType GetIndexType() const = 0;
    virtual IndexMerger* Create() const = 0;
};

DEFINE_SHARED_PTR(IndexMergerCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_MERGER_CREATOR_H
