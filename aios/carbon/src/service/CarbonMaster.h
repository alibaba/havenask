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
#ifndef CARBON_CARBONMASTER_H
#define CARBON_CARBONMASTER_H

#include "common/common.h"
#include "carbon/Log.h"
#include "carbon/proto/Carbon.pb.h"
#include "master/CarbonDriver.h"
#include "service/CarbonMasterServiceImpl.h"
#include "service/CarbonOpsServiceImpl.h"
#include "autil/Lock.h"
#include "worker_framework/LeaderedWorkerBase.h"

BEGIN_CARBON_NAMESPACE(service);

class CarbonMasterServiceImpl;
class CarbonMaster : public worker_framework::LeaderedWorkerBase {
public:
    CarbonMaster();
    virtual ~CarbonMaster();

private:    
    CarbonMaster(const CarbonMaster &);
    CarbonMaster& operator=(const CarbonMaster &);

protected:
    /* override */
    void doInitOptions();
    
    /* override */
    void doExtractOptions();
    
    /* override */
    bool doInit();

    /* override */    
    bool doStart();
    
    /* override */
    bool doPrepareToRun();

    /* override */
    void doIdle();

    /* override */
    void doStop();

    /* override */    
    void becomeLeader() {}

    /* override */    
    void noLongerLeader() {}
    
public:
    // public for test
    bool start();

public:
    std::string getZkPath() { return _zkPath;}
    
    std::string getHippoZkPath() { return _hippoZkPath;}
    
    std::string getAppId() { return _appId;}

    std::string getMasterAddr() { return _masterAddr; }
    
private:
    bool initRoleManager();
    
    bool checkIsNewStart(bool &isNewStart);
    
    bool deleteNewStartTag();

    bool checkAndDoUpdate();
    
private:
    CarbonMasterServiceImplPtr _carbonMasterServiceImplPtr;
    CarbonOpsServiceImplPtr _carbonOpsServiceImplPtr;
    master::CarbonDriverPtr _carbonDriverPtr;
    std::string _backupRoot;
    std::string _zkPath;
    std::string _hippoZkPath;
    std::string _masterAddr;
    std::string _appId;
    uint32_t _amonitorPort;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(CarbonMaster);

END_CARBON_NAMESPACE(master);

#endif //CARBON_CARBONMASTER_H
