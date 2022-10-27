#ifndef ISEARCH_SEEKRESOURCEVARIANT_H
#define ISEARCH_SEEKRESOURCEVARIANT_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include "ha3/search/SearchCommonResource.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/variant_encode_decode.h"

BEGIN_HA3_NAMESPACE(turing);

class SeekResource {
public:
    HA3_NS(search)::SearchCommonResourcePtr commonResource;
    HA3_NS(search)::SearchPartitionResourcePtr partitionResource;
    HA3_NS(search)::SearchRuntimeResourcePtr runtimeResource;
    HA3_NS(search)::LayerMetasPtr layerMetas;
    ~SeekResource() {
        commonResource.reset();
        partitionResource.reset();
    }

    void setTracer(const suez::turing::TracerPtr &tracer_)
    {
        tracer = tracer_;
    }
    void setTimeoutTerminator(const suez::turing::TimeoutTerminatorPtr &terminator_)
    {
        terminator = terminator_;
    }
    void setMetricsCollector(
            const HA3_NS(monitor)::SessionMetricsCollectorPtr &metricsCollector_)
    {
        metricsCollector = metricsCollector_;
    }    

private: //only hold life time
    suez::turing::TracerPtr tracer;
    suez::turing::TimeoutTerminatorPtr terminator;
    HA3_NS(monitor)::SessionMetricsCollectorPtr metricsCollector;
};

HA3_TYPEDEF_PTR(SeekResource);

class SeekResourceVariant {
public:
    SeekResourceVariant() {}
    SeekResourceVariant(const SeekResourcePtr &seekResource)
        : _seekResource(seekResource)
    {
    }
    ~SeekResourceVariant() {}

public:
    void Encode(tensorflow::VariantTensorData* data) const {}
    bool Decode(const tensorflow::VariantTensorData& data) {
        return false;
    }
    std::string TypeName() const {
        return "SeekResource";
    }
public:
    SeekResourcePtr getSeekResource() const {
        return _seekResource;
    }
private:
    SeekResourcePtr _seekResource;
private:
    HA3_LOG_DECLARE();

};

HA3_TYPEDEF_PTR(SeekResourceVariant);

END_HA3_NAMESPACE(turing);

#endif //ISEARCH_SEEKRESOURCEVARIANT_H
