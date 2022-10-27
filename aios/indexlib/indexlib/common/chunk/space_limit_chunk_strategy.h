#ifndef __INDEXLIB_SPACE_LIMIT_CHUNK_STRATEGY_H
#define __INDEXLIB_SPACE_LIMIT_CHUNK_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/chunk/chunk_strategy.h"

IE_NAMESPACE_BEGIN(common);

class SpaceLimitChunkStrategy : public ChunkStrategy
{
public:
    SpaceLimitChunkStrategy(size_t spaceLimitThreshold)
        : mThreshold(spaceLimitThreshold)
    {}
    
    ~SpaceLimitChunkStrategy() {}
    
public:
    bool NeedFlush(const ChunkWriterPtr& writer) const override final
    {
        assert(writer);
        return writer->TotalValueLength() >= mThreshold;
    }

private:
    size_t mThreshold;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpaceLimitChunkStrategy);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SPACE_LIMIT_CHUNK_STRATEGY_H
