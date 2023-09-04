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
#include "aios/network/gig/multi_call/subscribe/SubscribeServiceManager.h"

#include "aios/network/gig/multi_call/subscribe/CM2SubscribeService.h"
#include "aios/network/gig/multi_call/subscribe/IstioSubscribeService.h"
#include "aios/network/gig/multi_call/subscribe/LocalSubscribeService.h"
#include "aios/network/gig/multi_call/subscribe/VIPSubscribeService.h"

using namespace std;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, SubscribeServiceManager);

SubscribeServiceManager::SubscribeServiceManager() {
}

SubscribeServiceManager::~SubscribeServiceManager() {
}

bool SubscribeServiceManager::clusterInfoNeedUpdate() {
    autil::ScopedReadLock lock(_subLock);
    for (const auto &service : _subServiceVec) {
        if (service->clusterInfoNeedUpdate()) {
            return true;
        }
    }
    return false;
}

bool SubscribeServiceManager::getClusterInfoMap(TopoNodeVec &topoNodeVec,
                                                HeartbeatSpecVec &heartbeatSpecs) {
    autil::ScopedReadLock lock(_subLock);
    for (const auto &service : _subServiceVec) {
        if (!service->isEnable()) {
            AUTIL_LOG(DEBUG, "disabled subscribe service type:%u", service->getType());
            continue;
        }
        if (!service->getClusterInfoMap(topoNodeVec, heartbeatSpecs)) {
            REPORT_METRICS(reportSubUpdateFailedQps);
            AUTIL_LOG(DEBUG, "Error on service->getClusterInfoMap.");
            return false;
        } else {
            REPORT_METRICS(reportSubUpdateSuccQps);
        }
    }
    return true;
}

bool SubscribeServiceManager::addSubscribeService(const SubscribeConfig &config) {
    bool cm2Enabled = false;
    bool vipEnabled = false;
    bool istioEnabled = false;
    bool localEnabled = false;
    if (!config.validate(cm2Enabled, vipEnabled, localEnabled, istioEnabled)) {
        AUTIL_LOG(ERROR, "config is not valid");
        return false;
    }
    autil::ScopedWriteLock lock(_subLock);
    if (cm2Enabled) {
        std::vector<Cm2Config> cm2Configs;
        if (config.getCm2Configs(cm2Configs)) {
            for (auto &cm2Config : cm2Configs) {
                auto service = SubscribeServiceFactory::createCm2Sub(cm2Config);
                if (!service->init()) {
                    AUTIL_LOG(ERROR, "CM2 subscribe wrapper init failed, config [%s]!",
                              ToJsonString(cm2Config, true).c_str());
                    _subServiceVec.clear();
                    return false;
                }
                _subServiceVec.push_back(service);
            }
        }
    }
    if (vipEnabled) {
        auto service = SubscribeServiceFactory::createVipSub(config.vipConfig);
        if (!service->init()) {
            AUTIL_LOG(ERROR, "vip subscribe wrapper init failed, config [%s]!",
                      ToJsonString(config.vipConfig, true).c_str());
            _subServiceVec.clear();
            return false;
        }
        _subServiceVec.push_back(service);
    }
    if (istioEnabled) {
        IstioMetricReporterPtr reporter;
        if (_metricReporterManager) {
            reporter = _metricReporterManager->getIstioMetricReporter();
        }
        std::vector<IstioConfig> istioConfigs;
        if (config.getIstioConfigs(istioConfigs)) {
            for (auto &istioConfig : istioConfigs) {
                auto service = SubscribeServiceFactory::createIstioSub(istioConfig, reporter);
                if (!service->init()) {
                    AUTIL_LOG(ERROR, "istio subscribe wrapper init failed, config [%s]!",
                              ToJsonString(istioConfig, true).c_str());
                    _subServiceVec.clear();
                    return false;
                }
                _subServiceVec.push_back(service);
            }
        }
    }
    if (localEnabled) {
        _subServiceVec.push_back(SubscribeServiceFactory::createLocalSub(config.localConfig));
    }
    if (_subServiceVec.empty()) {
        if (!config.allowEmptySub) {
            AUTIL_LOG(ERROR, "empty subService not allowed, init failed");
            return false;
        } else {
            AUTIL_LOG(WARN, "subService is empty");
        }
    }
    return true;
}

bool SubscribeServiceManager::addSubscribe(const SubscribeClustersConfig &gigSubConf) {
    bool result = true;
    bool cm2Add = false;
    bool vipAdd = false;
    bool istioAdd = false;
    autil::ScopedReadLock lock(_subLock);
    for (SubscribeServicePtr &subService : _subServiceVec) {
        if (subService->getType() == ST_CM2 && gigSubConf.hasCm2Sub()) {
            CM2SubscribeServicePtr cm2SubService =
                dynamic_pointer_cast<CM2SubscribeService>(subService);
            result &= cm2SubService->addSubscribe(gigSubConf.cm2Clusters);
            cm2SubService->addTopoCluster(gigSubConf.cm2Clusters);
            result &= cm2SubService->addSubscribe(gigSubConf.cm2NoTopoClusters);
            cm2Add = true;
        } else if (subService->getType() == ST_VIP && gigSubConf.hasVipSub()) {
            VIPSubscribeServicePtr vipSubService =
                dynamic_pointer_cast<VIPSubscribeService>(subService);
            if (!vipSubService) {
                result = false;
            }
            result &= vipSubService->addSubscribeDomainConfig(gigSubConf.vipDomains);
            vipAdd = true;
        } else if (subService->getType() == ST_ISTIO && gigSubConf.hasIstioSub()) {
            result &= subService->addSubscribe(gigSubConf.istioBizClusters);
            istioAdd = true;
        } else if (subService->getType() == ST_LOCAL) { // rewrite local config
            LocalSubscribeServicePtr localSubService =
                dynamic_pointer_cast<LocalSubscribeService>(subService);
            if (localSubService == nullptr) {
                AUTIL_LOG(WARN, "local subService is empty");
                result = false;
            }
            bool enable = false;
            if (!gigSubConf.localConfig.validate(enable)) {
                result = false;
            } else if (enable) {
                localSubService->updateLocalConfig(gigSubConf.localConfig);
            }
        }
    }
    result &= gigSubConf.checkSub(cm2Add, vipAdd, istioAdd);
    return result;
}

bool SubscribeServiceManager::deleteSubscribe(const SubscribeClustersConfig &gigSubConf) {
    bool result = true;
    bool cm2Del = false;
    bool vipDel = false;
    bool istioDel = false;
    autil::ScopedReadLock lock(_subLock);
    for (SubscribeServicePtr &subService : _subServiceVec) {
        if (subService->getType() == ST_CM2) {
            CM2SubscribeServicePtr cm2SubService =
                dynamic_pointer_cast<CM2SubscribeService>(subService);
            result &= cm2SubService->deleteSubscribe(gigSubConf.cm2Clusters);
            cm2SubService->deleteTopoCluster(gigSubConf.cm2Clusters);
            result &= cm2SubService->deleteSubscribe(gigSubConf.cm2NoTopoClusters);
            cm2Del = true;
        } else if (subService->getType() == ST_VIP) {
            VIPSubscribeServicePtr vipSubService =
                dynamic_pointer_cast<VIPSubscribeService>(subService);
            if (!vipSubService) {
                result = false;
            }
            result &= vipSubService->deleteSubscribeDomainConfig(gigSubConf.vipDomains);
            vipDel = true;
        } else if (subService->getType() == ST_ISTIO) {
            result &= subService->deleteSubscribe(gigSubConf.istioBizClusters);
            istioDel = true;
        }
    }

    result &= gigSubConf.checkSub(cm2Del, vipDel, istioDel);
    return result;
}

void SubscribeServiceManager::enableSubscribeService(SubscribeType ssType) {
    AUTIL_LOG(INFO, "to enable subscribe service type:%s",
              convertSubscribeTypeToStr(ssType).c_str());
    autil::ScopedReadLock lock(_subLock);
    for (SubscribeServicePtr &subService : _subServiceVec) {
        if (subService->getType() == ssType) {
            subService->setEnable(true);
        }
    }
}

void SubscribeServiceManager::disableSubscribeService(SubscribeType ssType) {
    AUTIL_LOG(INFO, "to disable subscribe service type:%s",
              convertSubscribeTypeToStr(ssType).c_str());
    autil::ScopedReadLock lock(_subLock);
    for (SubscribeServicePtr &subService : _subServiceVec) {
        if (subService->getType() == ssType) {
            subService->setEnable(false);
        }
    }
}

void SubscribeServiceManager::stopSubscribeService(SubscribeType ssType) {
    AUTIL_LOG(INFO, "to stop subscribe service type:%s, only istio support",
              convertSubscribeTypeToStr(ssType).c_str());
    autil::ScopedReadLock lock(_subLock);
    for (SubscribeServicePtr &subService : _subServiceVec) {
        if (subService->getType() == ssType) {
            subService->stopSubscribe();
        }
    }
}

bool SubscribeServiceManager::subscribeBiz(const std::string &bizName) {
    if (bizName.empty()) {
        return false;
    }
    // only support biz subscribe in istio
    SubscribeClustersConfig subConf;
    subConf.istioBizClusters.push_back(bizName);
    if (!addSubscribe(subConf)) {
        AUTIL_LOG(ERROR, "subscribe biz:[%s] failed", bizName.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "subscribe biz:[%s] succeed", bizName.c_str());
    return true;
}

bool SubscribeServiceManager::unsubscribeBiz(const std::string &bizName) {
    if (bizName.empty()) {
        return false;
    } // only support biz subscribe in istio
    SubscribeClustersConfig subConf;
    subConf.istioBizClusters.push_back(bizName);
    if (!deleteSubscribe(subConf)) {
        AUTIL_LOG(ERROR, "unsubscribe biz:[%s] failed", bizName.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "unsubscribe biz:[%s] succeed", bizName.c_str());
    return true;
}

} // namespace multi_call
