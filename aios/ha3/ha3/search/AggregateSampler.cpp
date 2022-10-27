#include <ha3/search/AggregateSampler.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, AggregateSampler);

AggregateSampler::AggregateSampler(uint32_t aggThreshold,
                                   uint32_t sampleStep)
    :_aggThreshold(aggThreshold),
     _sampleStep(sampleStep)
{

    _aggCount = 0;
    _stepCount = 0;
}

AggregateSampler::~AggregateSampler() {
}


END_HA3_NAMESPACE(search);
