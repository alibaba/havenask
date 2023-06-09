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
#ifndef ISEARCH_MULTI_CALL_ERRORRATIOCONTROLLER_H
#define ISEARCH_MULTI_CALL_ERRORRATIOCONTROLLER_H

#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "aios/network/gig/multi_call/controller/RatioControllerBase.h"

namespace multi_call {

class ErrorRatioController : public RatioControllerBase {
public:
    ErrorRatioController();

private:
    ErrorRatioController(const ErrorRatioController &);
    ErrorRatioController &operator=(const ErrorRatioController &);

public:
    float controllerOutput() const override;
    void updateLoadBalance(ControllerFeedBack &feedBack);

public:
    bool isLegal(const ControllerChain *bestChain,
                 const MetricLimits &metricLimits) const override;
    float getLegalLimit(const FlowControlConfigPtr &flowControlConfig) const;

protected:
    void updateLocalRatio(const QueryResultStatistic &stat) override;
    const ServerRatioFilter &
    getServerRatioFilter(const GigResponseInfo &agentInfo) const override;
    const ServerRatioFilter &
    getMirrorServerRatioFilter(const PropagationStatDef &stat) const override;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ErrorRatioController);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_ERRORRATIOCONTROLLER_H
