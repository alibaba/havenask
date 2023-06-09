/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <string>

#include "autil/TimeoutTerminator.h"
#include "tensorflow/core/framework/variant_tensor_data.h"

#include "ha3/common/Tracer.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/SearchCommonResource.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace turing {

class SeekResource {
public:
    isearch::search::SearchCommonResourcePtr commonResource;
    isearch::search::SearchPartitionResourcePtr partitionResource;
    isearch::search::SearchRuntimeResourcePtr runtimeResource;
    isearch::search::LayerMetasPtr layerMetas;
    ~SeekResource() {
        commonResource.reset();
        partitionResource.reset();
    }

    void setTracer(const suez::turing::TracerPtr &tracer_)
    {
        tracer = tracer_;
    }
    void setTimeoutTerminator(const autil::TimeoutTerminatorPtr &terminator_)
    {
        terminator = terminator_;
    }
    void setMetricsCollector(
            const isearch::monitor::SessionMetricsCollectorPtr &metricsCollector_)
    {
        metricsCollector = metricsCollector_;
    }

private: //only hold life time
    suez::turing::TracerPtr tracer;
    autil::TimeoutTerminatorPtr terminator;
    isearch::monitor::SessionMetricsCollectorPtr metricsCollector;
};

typedef std::shared_ptr<SeekResource> SeekResourcePtr;

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
    AUTIL_LOG_DECLARE();

};

typedef std::shared_ptr<SeekResourceVariant> SeekResourceVariantPtr;

} // namespace turing
} // namespace isearch
