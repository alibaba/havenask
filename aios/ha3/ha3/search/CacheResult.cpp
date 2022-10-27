#include <ha3/search/CacheResult.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(common);

AUTIL_BEGIN_NAMESPACE(autil);
DECLARE_SIMPLE_TYPE(HA3_NS(index)::PartitionInfoHint);
AUTIL_END_NAMESPACE(autil);

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, CacheResult);

CacheResult::CacheResult()
    : _result(NULL)
    , _isTruncated(false)
    , _useTruncateOptimizer(false)
{
}

CacheResult::~CacheResult() {
    DELETE_AND_SET_NULL(_result);
}

Result* CacheResult::stealResult() {
    Result *tmp = _result;
    _result = NULL;
    return tmp;
}

void CacheResult::serialize(autil::DataBuffer &dataBuffer) const  {
    serializeHeader(dataBuffer);
    serializeResult(dataBuffer);
}

void CacheResult::serializeHeader(autil::DataBuffer &dataBuffer) const  {
    dataBuffer.write(_header.expireTime);
    dataBuffer.write(_header.minScoreFilter);
    dataBuffer.write(_header.time);
}

void CacheResult::serializeResult(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_partInfoHint);
    dataBuffer.write(_gids);
    dataBuffer.write(_isTruncated);
    dataBuffer.write(_useTruncateOptimizer);
    bool hasSubDoc = !_subDocGids.empty();
    dataBuffer.write(hasSubDoc);
    if (hasSubDoc) {
        dataBuffer.write(_subDocGids);
    }

    // write result
    uint8_t originalSerializeLevel = SL_NONE;
    if (_result) {
        // skip some reference which dose not need serialize value
        // such as DocIdentifier/ip/FullIndexVersion...
        originalSerializeLevel = _result->getSerializeLevel();
        _result->setSerializeLevel(SL_CACHE);
    }
    dataBuffer.write(_result);
    if (_result) {
        _result->setSerializeLevel(originalSerializeLevel);
    }
}

void CacheResult::deserialize(autil::DataBuffer &dataBuffer,
                              autil::mem_pool::Pool *pool)
{
    deserializeHeader(dataBuffer);
    deserializeResult(dataBuffer, pool);
}

void CacheResult::deserializeHeader(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_header.expireTime);
    dataBuffer.read(_header.minScoreFilter);
    dataBuffer.read(_header.time);
}

void CacheResult::deserializeResult(autil::DataBuffer &dataBuffer,
                                    autil::mem_pool::Pool *pool)
{
    dataBuffer.read(_partInfoHint);
    dataBuffer.read(_gids);

    dataBuffer.read(_isTruncated);
    dataBuffer.read(_useTruncateOptimizer);

    bool hasSubDoc = false;
    dataBuffer.read(hasSubDoc);
    if (hasSubDoc) {
        dataBuffer.read(_subDocGids);
    }

    bool bResultExist = false;
    dataBuffer.read(bResultExist);
    if (!bResultExist) {
        return;
    }

    _result = new Result();
    assert(_result != NULL);
    uint8_t originalSerializeLevel = _result->getSerializeLevel();
    _result->setSerializeLevel(SL_CACHE);
    _result->deserialize(dataBuffer, pool);
    _result->setSerializeLevel(originalSerializeLevel);
}

END_HA3_NAMESPACE(search);
