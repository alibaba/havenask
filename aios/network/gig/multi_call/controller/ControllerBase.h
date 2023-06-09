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
#ifndef ISEARCH_MULTI_CALL_CONTROLLERBASE_H
#define ISEARCH_MULTI_CALL_CONTROLLERBASE_H

#include "aios/network/gig/multi_call/controller/ControllerFeedBack.h"
#include "aios/network/gig/multi_call/util/Filter.h"

namespace multi_call {

class AverageLatency;

class ControllerBase {
public:
    ControllerBase(const std::string &controllerName);
    virtual ~ControllerBase();

private:
    ControllerBase(const ControllerBase &);
    ControllerBase &operator=(const ControllerBase &);

public:
    virtual float controllerOutput() const = 0;

public:
    bool isValid() const;
    int64_t count() const;

    bool hasServerAgent() const;
    void setHasServerAgent(bool value);

    bool serverFilterReady() const;
    void setServerFilterReady(bool value);

    void setServerValue(float value);
    void setServerValueVersion(int64_t version);
    bool serverValueReady() const;
    float serverValue() const;
    int64_t serverValueVersion() const;
    float serverValueDelayMs() const;
    void clearServerValue();

    void setLoadBalanceServerValue(float value);
    float loadBalanceServerValue() const;

    void setServerAvgWeight(float weight);
    float serverAvgWeight() const;

    void updateLocal(float value);
    float localValue() const;

    bool needProbe() const;
    CompareFlag compare(const ControllerBase &rhs, float allow = 0.0f) const;
    bool invalidServerVersion(const AverageLatency &avgLatency) const;

public:
    // for ut
    void setCurrent(float current) { _localFilter.setCurrent(current); }
    void setFilterCounter(int64_t counter) { _localFilter.setCounter(counter); }

private:
    std::string _controllerName;
    bool _hasServerAgent;
    bool _serverFilterReady;
    Filter _localFilter;
    int64_t _serverValueVersion;
    float _serverValueDelay;
    float _serverValue;
    float _loadBalanceServerValue;
    float _serverAvgWeight;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ControllerBase);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CONTROLLERBASE_H
