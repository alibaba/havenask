#include <ha3/util/ZlibCompressorWrapper.h>

BEGIN_HA3_NAMESPACE(util);
HA3_LOG_SETUP(util, ZlibCompressorWrapper);
using namespace std;
using namespace autil;

ZlibCompressorWrapper::ZlibCompressorWrapper(uint32_t bufferInLen, uint32_t bufferOutLen)
{
    _compressor.setBufferInLen(bufferInLen);
    _compressor.setBufferOutLen(bufferOutLen);
}

bool ZlibCompressorWrapper::compress(const std::string& sourceString, std::string& destString)
{
    _compressor.reset();
    _compressor.addDataToBufferIn(sourceString.c_str(), sourceString.length());
    if (!_compressor.compress()) {
        HA3_LOG(WARN, "failed to compress sourceString : %s", sourceString.c_str());
        return false;
    }
    
    destString.assign(_compressor.getBufferOut(), _compressor.getBufferOutLen());
    return true;
}
bool ZlibCompressorWrapper::decompress(const std::string& sourceString, std::string& destString)
{
    _compressor.reset();
    _compressor.addDataToBufferIn(sourceString.c_str(), sourceString.length());
    if (!_compressor.decompress()) {
        HA3_LOG(WARN, "decompress string[%s] fail.", sourceString.c_str());
        return false;
    }
    destString.assign(_compressor.getBufferOut(), _compressor.getBufferOutLen());
    return true;
}


END_HA3_NAMESPACE(util);

