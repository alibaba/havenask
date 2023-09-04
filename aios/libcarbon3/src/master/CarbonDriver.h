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
#include "monitor/CarbonMonitor.h"
#include "carbon/Log.h"
#include "master/SerializerCreator.h"
#include "master/GlobalVariable.h"
#include "master/GlobalConfigManager.h"
#include "worker_framework/LeaderElector.h"
#include "autil/Lock.h"
#include "master/ProxyClient.h"

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
              const std::string &backupRoot,
              worker_framework::LeaderElector *leaderElector,
              const std::string &proxySpec = "", const std::string &kmonSpec = "",
              const DriverOptions& opts = DriverOptions());

    bool start();

    void stop();

    ProxyClientPtr getProxy() const {
        return _proxyPtr;
    }

    GlobalConfigManagerPtr getGlobalConfigManager() const {
        return _globalConfigManagerPtr;
    }

    CarbonInfo getCarbonInfo() const;

    bool isInited() const;

private:
    bool initHippoAdapter(const std::string &applicationId,
                          const std::string &hippoZk,
                          worker_framework::LeaderElector *leaderElector);

    bool initMonitor(const std::string &appId, const std::string& kmonSpec);
    
    bool newStart(const std::string &serializePath);

    bool recover();
        
    bool recover(bool isAppRestart);

    bool removeSerializeDir(const std::string &serializePath);
    
private:
    SerializerCreatorPtr _serializerCreatorPtr;
    std::string _mode;
    ProxyClientPtr _proxyPtr;
    GlobalConfigManagerPtr _globalConfigManagerPtr;
    volatile bool _initRet;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonDriver);

END_CARBON_NAMESPACE(master);

#endif //CARBON_CARBONDRIVER_H
