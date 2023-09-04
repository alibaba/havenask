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
#ifndef ISEARCH_MULTI_CALL_RATIOCONTROLLERBASE_H
#define ISEARCH_MULTI_CALL_RATIOCONTROLLERBASE_H

#include "aios/network/gig/multi_call/controller/ControllerBase.h"

namespace multi_call {

class ServerRatioFilter;
class GigResponseInfo;
class PropagationStatDef;

class RatioControllerBase : public ControllerBase
{
public:
    RatioControllerBase(const std::string &controllerName, float decStep);

private:
    RatioControllerBase(const RatioControllerBase &);
    RatioControllerBase &operator=(const RatioControllerBase &);

public:
    float update(ControllerFeedBack &feedBack);

protected:
    virtual void updateLocalRatio(const QueryResultStatistic &stat) = 0;
    virtual bool isLegal(const ControllerChain *bestChain,
                         const MetricLimits &metricLimits) const = 0;
    virtual const ServerRatioFilter &
    getServerRatioFilter(const GigResponseInfo &agentInfo) const = 0;
    virtual const ServerRatioFilter &
    getMirrorServerRatioFilter(const PropagationStatDef &stat) const = 0;

protected:
    void updateServerRatio(const QueryResultStatistic &stat);
    void updateServerRatioMirror(const QueryResultStatistic &stat);

private:
    void doUpdateServerRatio(const ServerRatioFilter &serverRatioFilter);

private:
    float _decStep;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(RatioControllerBase);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_RATIOCONTROLLERBASE_H
