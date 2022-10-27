#ifndef ISEARCH_HACOMPRESSUTIL_H
#define ISEARCH_HACOMPRESSUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/ZlibCompressor.h>
#include <autil/mem_pool/Pool.h>
#include <snappy.h>
#include <lz4.h>
BEGIN_HA3_NAMESPACE(util);

class HaCompressUtil
{
public:
    HaCompressUtil();
    ~HaCompressUtil();
private:
    HaCompressUtil(const HaCompressUtil &);
    HaCompressUtil& operator=(const HaCompressUtil &);
public:
    static bool compress(const std::string &result,
                         HaCompressType type,
                         std::string &compressedResult,
                         autil::mem_pool::Pool *pool = NULL);

    static bool decompress(const std::string &result,
                           HaCompressType type,
                           std::string &decompressedResult,
                           autil::mem_pool::Pool *pool = NULL);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(HaCompressUtil);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_HACOMPRESSUTIL_H
