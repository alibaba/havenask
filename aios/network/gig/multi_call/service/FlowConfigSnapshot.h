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
#ifndef ISEARCH_MULTI_CALL_FLOWCONFIGSNAPSHOT_H
#define ISEARCH_MULTI_CALL_FLOWCONFIGSNAPSHOT_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "autil/legacy/json.h"

namespace multi_call {

class FlowConfigSnapshot {
public:
    FlowConfigSnapshot();
    ~FlowConfigSnapshot();

private:
    FlowConfigSnapshot(const FlowConfigSnapshot &);
    FlowConfigSnapshot &operator=(const FlowConfigSnapshot &);

public: // only for create snapshot for old snapshot
    void updateDefaultFlowConfig(const FlowControlConfigPtr &flowControlConfig);
    void updateFlowConfig(const std::string &strategy,
                          const FlowControlConfigPtr &flowControlConfig);
    std::shared_ptr<FlowConfigSnapshot> clone();

public:
    FlowControlConfigPtr
    getFlowControlConfig(const std::string &strategy) const;
    bool getFlowControlConfig(const std::string &strategy,
                              FlowControlConfigPtr &config) const;
    void getFlowControlSwitch(const std::vector<std::string> &strategyVec,
                              bool &earlyTermination, bool &retry,
                              bool &singleRetry) const;
    const FlowControlConfigMap &getConfigMap() const { return *_configMap; }
    std::string toString() const;

private:
    FlowControlConfigMapPtr _configMap;
    FlowControlConfigPtr _defaultConfig;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(FlowConfigSnapshot);
} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_FLOWCONFIGSNAPSHOT_H
