#ifndef __INDEXLIB_CHUNK_ENCODER_H
#define __INDEXLIB_CHUNK_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

// TODO: support later
class ChunkEncoder
{
public:
    ChunkEncoder();
    virtual ~ChunkEncoder();
public:

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ChunkEncoder);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CHUNK_ENCODER_H
