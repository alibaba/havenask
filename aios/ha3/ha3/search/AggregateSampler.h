#ifndef ISEARCH_AGGREGATESAMPLER_H
#define ISEARCH_AGGREGATESAMPLER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>

BEGIN_HA3_NAMESPACE(search);

class AggregateSampler
{
public:
    AggregateSampler(uint32_t aggThreshold, uint32_t sampleStep);
    ~AggregateSampler();
public:
    inline bool sampling();
    inline uint32_t getFactor() const;
    inline bool isBeginOfSample() const;
    uint32_t getAggregateCount() {
        return _aggCount;
    }
    const uint32_t getSampleStep() {
        return _sampleStep;
    }
private:
    const uint32_t _aggThreshold;
    const uint32_t _sampleStep;
    uint32_t _aggCount;
    uint32_t _stepCount;
private:
    HA3_LOG_DECLARE();
};

bool AggregateSampler::sampling() {
    if (_aggCount < _aggThreshold) {
        _aggCount++;
        return true;
    }
    
    _stepCount++;
    if (_stepCount == _sampleStep) {
        _stepCount = 0;
        _aggCount++;
        return true;
    }
        
    return false;
}

bool AggregateSampler::isBeginOfSample() const {
    return _aggCount  == _aggThreshold && _stepCount == 1;
}

uint32_t AggregateSampler::getFactor() const {
    if(_aggCount <= _aggThreshold
       && _stepCount == 0) 
    {
        return 1;
    }
    return _sampleStep;
}

HA3_TYPEDEF_PTR(AggregateSampler);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_AGGREGATESAMPLER_H
