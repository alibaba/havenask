#ifndef ISEARCH_ZLIBCOMPRESSORWRAPPER_H
#define ISEARCH_ZLIBCOMPRESSORWRAPPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/ZlibCompressor.h>

BEGIN_HA3_NAMESPACE(util);

class ZlibCompressorWrapper
{
public:
    ZlibCompressorWrapper(uint32_t bufferInLen, uint32_t bufferOutLen);
    ~ZlibCompressorWrapper() {}

public:
    bool compress(const std::string& sourceString, std::string& destString);
    bool decompress(const std::string& sourceString, std::string& destString);

private:
    ZlibCompressorWrapper(const ZlibCompressorWrapper &);
    ZlibCompressorWrapper& operator=(const ZlibCompressorWrapper &);

private:
    autil::ZlibCompressor _compressor;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ZlibCompressorWrapper);

END_HA3_NAMESPACE(util);

#endif //ISEARCH_ZLIBCOMPRESSORWRAPPER_H
