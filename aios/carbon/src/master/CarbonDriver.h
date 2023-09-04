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
#ifndef CARBON_CARBONDRIVER_H
#define CARBON_CARBONDRIVER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/GlobalConfig.h"
#include "master/GroupPlanManager.h"
#include "master/GroupManager.h"
#include "master/HippoAdapter.h"
#include "master/SerializerCreator.h"
#include "master/GlobalVariable.h"
#include "master/Router.h"
#include "master/SysConfigManager.h"
#include "monitor/CarbonMonitorSingleton.h"
#include "worker_framework/LeaderElector.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(master);

class CarbonDriver
{
public:
    CarbonDriver(const std::string &mode = CARBON_MODE_LIB);
    ~CarbonDriver();
private:
    CarbonDriver(const CarbonDriver &);
    CarbonDriver& operator=(const CarbonDriver &);
public:
    bool init(const std::string &applicationId,
              const std::string &hippoZk,
              const std::string &carbonZk,
              const std::string &backupPath,
              worker_framework::LeaderElector *leaderElector,
              bool isNewStart,
              uint32_t amonitorPort, bool withBuffer = false, bool k8sMode = false);

    bool start();

    void stop();

    GroupPlanManagerPtr getGroupPlanManager() const {
        return _groupPlanManagerPtr;
    }
    
    GroupManagerPtr getGroupManager() const {
        return _groupManagerPtr;
    }

    RouterPtr getRouter() const {
        return _routerPtr;
    }

    CarbonInfo getCarbonInfo() const;

    bool isInited() const;

    void setGlobalConfig(const GlobalConfig& config, ErrorInfo* errorInfo);
    GlobalConfig getGlobalConfig() const;
    HippoAdapterPtr getBufferHippoAdapter(); 

    DriverStatus getStatus(ErrorInfo* errorInfo) const;

    void setExtServiceAdapterCreator(ServiceAdapterExtCreatorPtr creator) {
        _extAdapterCreatorPtr = creator;
    }

private:
    bool initHippoAdapter(const std::string &applicationId,
                          const std::string &hippoZk,
                          worker_framework::LeaderElector *leaderElector);

    bool initGroupManager(const std::string& app);

    bool initMonitor(const std::string &appId, uint32_t port);
    
    bool newStart(const std::string &serializePath);

    bool recover();
        
    bool recover(bool isAppRestart);

    bool removeSerializeDir(const std::string &serializePath);

    bool checkAppRestart(bool *isAppRestart);

    bool writeAppChecksum();
    
private:
    HippoAdapterPtr _hippoAdapterPtr;
    SerializerCreatorPtr _serializerCreatorPtr;
    GroupPlanManagerPtr _groupPlanManagerPtr;
    GroupManagerPtr _groupManagerPtr;
    ServiceAdapterExtCreatorPtr _extAdapterCreatorPtr;
    RouterPtr _routerPtr;
    SysConfigManagerPtr _sysConfManagerPtr;
    std::string _mode;
    volatile bool _initRet;
    bool _withBuffer;
    bool _k8sMode;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonDriver);

END_CARBON_NAMESPACE(master);

#endif //CARBON_CARBONDRIVER_H
