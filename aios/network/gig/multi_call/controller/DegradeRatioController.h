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
#ifndef ISEARCH_MULTI_CALL_DEGRADERATIOCONTROLLER_H
#define ISEARCH_MULTI_CALL_DEGRADERATIOCONTROLLER_H

#include "aios/network/gig/multi_call/controller/RatioControllerBase.h"

namespace multi_call {

class DegradeRatioController : public RatioControllerBase
{
public:
    DegradeRatioController();

private:
    DegradeRatioController(const DegradeRatioController &);
    DegradeRatioController &operator=(const DegradeRatioController &);

public:
    float updateLoadBalance(ControllerFeedBack &feedBack);
    float controllerOutput() const override;
    float controllerLoadBalanceOutput() const;
    bool isLegal(const ControllerChain *bestChain, const MetricLimits &metricLimits) const override;
    float getLegalLimit() const;

protected:
    void updateLocalRatio(const QueryResultStatistic &stat) override;
    const ServerRatioFilter &getServerRatioFilter(const GigResponseInfo &agentInfo) const override;
    const ServerRatioFilter &
    getMirrorServerRatioFilter(const PropagationStatDef &stat) const override;
    bool getBestRatio(const ControllerFeedBack &feedBack, float &bestRatio) const;

private:
    static float calculateWeightPercent(float thisRatio, float bestRatio);

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(DegradeRatioController);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_DEGRADERATIOCONTROLLER_H
