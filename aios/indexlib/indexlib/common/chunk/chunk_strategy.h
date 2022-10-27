#ifndef __INDEXLIB_CHUNK_STRATEGY_H
#define __INDEXLIB_CHUNK_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/chunk/chunk_writer.h"

IE_NAMESPACE_BEGIN(common);

class ChunkStrategy
{
public:
    ChunkStrategy() {}
    virtual ~ChunkStrategy() {}
    
public:
    virtual bool NeedFlush(const ChunkWriterPtr& writer) const = 0;
};

DEFINE_SHARED_PTR(ChunkStrategy);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CHUNK_STRATEGY_H
