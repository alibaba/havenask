#ifndef __INDEXLIB_CHUNK_DEFINE_H
#define __INDEXLIB_CHUNK_DEFINE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

struct ChunkMeta
{
    uint32_t length : 24;
    uint32_t isEncoded : 8;
};

const uint64_t MAX_CHUNK_DATA_LEN = (size_t)(1 << 24) - 1;

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_CHUNK_DEFINE_H
