#ifndef __INDEXLIB_INDEX_READER_FACTORY_H
#define __INDEXLIB_INDEX_READER_FACTORY_H

#include <map>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, IndexReader);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);

IE_NAMESPACE_BEGIN(index);

class IndexReaderFactory
{
public:
    static IndexReader* CreateIndexReader(IndexType indexType);

    static PrimaryKeyIndexReader* CreatePrimaryKeyIndexReader(
            IndexType indexType);
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_READER_FACTORY_H
