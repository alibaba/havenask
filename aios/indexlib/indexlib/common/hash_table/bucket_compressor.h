#ifndef __INDEXLIB_BUCKET_COMPRESSOR_H
#define __INDEXLIB_BUCKET_COMPRESSOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(common);

class BucketCompressor
{
public:
    BucketCompressor() {}
    virtual ~BucketCompressor() {}
public:
    virtual size_t Compress(char* in, size_t bucketSize, char* out) = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BucketCompressor);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_BUCKET_COMPRESSOR_H
