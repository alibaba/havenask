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
#ifndef ISEARCH_MULTI_CALL_LOCALSUBSCRIBESERVICE_H
#define ISEARCH_MULTI_CALL_LOCALSUBSCRIBESERVICE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/subscribe/SubscribeService.h"

namespace multi_call {

class LocalSubscribeService : public SubscribeService
{
public:
    LocalSubscribeService(const LocalConfig &localConfig) : _localConfig(localConfig) {
    }
    ~LocalSubscribeService() {
    }

private:
    LocalSubscribeService(const LocalSubscribeService &);
    LocalSubscribeService &operator=(const LocalSubscribeService &);

public:
    bool init() override {
        return true;
    }
    SubscribeType getType() override {
        return ST_LOCAL;
    }
    bool clusterInfoNeedUpdate() override {
        return true;
    }
    void updateLocalConfig(const LocalConfig &localConfig) {
        autil::ScopedWriteLock lock(_configLock);
        _localConfig = localConfig;
    }
    bool getClusterInfoMap(TopoNodeVec &topoNodeVec, HeartbeatSpecVec &heartbeatSpecs) override {
        autil::ScopedReadLock lock(_configLock);
        for (const auto &node : _localConfig.nodes) {
            auto topoNode = node.getTopoNode();
            if (node.supportHeartbeat()) {
                HeartbeatSpec spec;
                spec.spec = topoNode.spec;
                heartbeatSpecs.push_back(spec);
            } else {
                if (!topoNode.bizName.empty()) {
                    topoNodeVec.push_back(topoNode);
                }
            }
        }
        return true;
    }

private:
    autil::ReadWriteLock _configLock;
    LocalConfig _localConfig;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(LocalSubscribeService);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_LOCALSUBSCRIBESERVICE_H
