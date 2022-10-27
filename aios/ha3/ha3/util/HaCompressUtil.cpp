#include <ha3/util/HaCompressUtil.h>

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, HaCompressUtil);

HaCompressUtil::HaCompressUtil() { 
}

HaCompressUtil::~HaCompressUtil() { 
}

bool HaCompressUtil::compress(const std::string &result,
                              HaCompressType type,
                              std::string &compressedResult,
                              autil::mem_pool::Pool *pool)
{
    assert(type != INVALID_COMPRESS_TYPE);
    switch (type) {
    case SNAPPY:
        return snappy::Compress(result.c_str(), result.length(), &compressedResult);
    case Z_SPEED_COMPRESS:
    case Z_DEFAULT_COMPRESS: {
        int compressType = Z_SPEED_COMPRESS == type ?
                           Z_BEST_SPEED : Z_DEFAULT_COMPRESSION;
        autil::ZlibCompressor compressor(compressType);
        compressor.addDataToBufferIn(result);
        if (false == compressor.compress()) {
            HA3_LOG(WARN, "ZlibCompressor compress failed!");
            return false;
        }
        compressedResult.assign(compressor.getBufferOut(),
                                compressor.getBufferOutLen());
        return true;
    }
    case LZ4: {
        uint32_t sourceSize = result.length();
        int maxDestSize = LZ4_compressBound(sourceSize);
        if (0 == maxDestSize) {
            HA3_LOG(WARN, "LZ4_compressBound failed!"
                    " maybe input size is too large!");
            return false;
        }
        // header compress length
        char *dest = POOL_COMPATIBLE_NEW_VECTOR(pool, char,
                maxDestSize + sizeof(int));
        int compSize = LZ4_compress_fast(result.c_str(), dest + sizeof(int),
                sourceSize, maxDestSize, 1);
        if (0 == compSize) {
            HA3_LOG(WARN, "LZ4_compress_default failed!");
            POOL_COMPATIBLE_DELETE_VECTOR(pool, dest);
            return false;
        }
        *(int *)dest = sourceSize;
        compressedResult.assign(dest, compSize + sizeof(int));
        POOL_COMPATIBLE_DELETE_VECTOR(pool, dest);
        return true;
    }
    case NO_COMPRESS:
        return false;
    default:
        return false;
    }
}

bool HaCompressUtil::decompress(const std::string &result,
                                HaCompressType type,
                                std::string &decompressedResult,
                                autil::mem_pool::Pool *pool)
{
    assert(type != INVALID_COMPRESS_TYPE);
    switch (type) {
    case SNAPPY:
        return snappy::Uncompress(result.c_str(), result.length(), &decompressedResult);
    case Z_SPEED_COMPRESS:
    case Z_DEFAULT_COMPRESS: {
        int compressType = Z_SPEED_COMPRESS == type ?
                           Z_BEST_SPEED : Z_DEFAULT_COMPRESSION;
        autil::ZlibCompressor compressor(compressType);
        compressor.addDataToBufferIn(result);
        if (false == compressor.decompress()) {
            HA3_LOG(WARN, "ZlibCompressor decompress failed!");
            return false;
        }
        decompressedResult.assign(compressor.getBufferOut(),
                                compressor.getBufferOutLen());
        return true;
    }
    case LZ4: {
        if (result.length() < 4) {
            HA3_LOG(WARN, "compressed result length must large than 4 bytes for header!");
            return false;
        }
        int originalSize = *(int*)result.c_str();
        char *dest = POOL_COMPATIBLE_NEW_VECTOR(pool, char,
                originalSize + 1);
        int uncompRet = LZ4_decompress_fast(result.c_str() + sizeof(int), dest, originalSize);
        if (uncompRet < 0) {
            HA3_LOG(WARN, "LZ4_decompress_fast failed!");
            POOL_COMPATIBLE_DELETE_VECTOR(pool, dest);
            return false;
        }
        decompressedResult.assign(dest, originalSize);
        POOL_COMPATIBLE_DELETE_VECTOR(pool, dest);
        return true;
    }
    case NO_COMPRESS:
        return false;
    default:
        return false;
    }
}

END_HA3_NAMESPACE(util);

