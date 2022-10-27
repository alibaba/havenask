#include <ha3/common/CompressTypeConvertor.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, CompressTypeConvertor);

CompressTypeConvertor::CompressTypeConvertor() { 
}

CompressTypeConvertor::~CompressTypeConvertor() { 
}

HaCompressType CompressTypeConvertor::convertCompressType(
        const std::string &compressTypeStr)
{
    if (compressTypeStr == "no_compress") {
        return NO_COMPRESS;
    } else if (compressTypeStr == "z_speed_compress") {
        return Z_SPEED_COMPRESS;
    } else if (compressTypeStr == "z_default_compress") {
        return Z_DEFAULT_COMPRESS;
    } else if (compressTypeStr == "snappy") {
        return SNAPPY;
    } else if (compressTypeStr == "lz4") {
        return LZ4;
    }
    
    return INVALID_COMPRESS_TYPE;
}

END_HA3_NAMESPACE(common);

