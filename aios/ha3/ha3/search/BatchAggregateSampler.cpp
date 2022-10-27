#include <ha3/search/BatchAggregateSampler.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, BatchAggregateSampler);

BatchAggregateSampler::BatchAggregateSampler(
        uint32_t aggThreshold, uint32_t sampleMaxCount)
    : _aggThreshold(aggThreshold), _sampleMaxCount(sampleMaxCount), _maxSubDocNum(0)
{
    reStart();
    _docids.reserve(DEFAULT_DOCIDS_INIT_SIZE);
}

BatchAggregateSampler::~BatchAggregateSampler() { 
}

void BatchAggregateSampler::reset() {
    _docids.clear();
    _subdocidEnds.clear();
    _subdocids.clear();
    _multiLayerPos.clear();
    _multiLayerFactor.clear();
    reStart();
}

void BatchAggregateSampler::calculateSampleStep() {
    size_t count = _docids.size();
    if (count == 0) {
        return;
    }

    if(_aggThreshold == 0 && _sampleMaxCount == 0) {
        _step = 1;
        _aggDocCount = count;
        return;
    }

    if (count > _aggThreshold && _sampleMaxCount == 0) {
        _curLayerIdx = _multiLayerPos.size();
        return;
    }

    if (count <= _aggThreshold) {
        _step = 1;
        _aggDocCount = count;
    } else {
        _step = (count + _sampleMaxCount - 1) / _sampleMaxCount;
        _aggDocCount = (count + _step - 1) / _step;
    }
}


END_HA3_NAMESPACE(search);

