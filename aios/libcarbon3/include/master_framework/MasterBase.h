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
#ifndef MASTER_FRAMEWORK_MASTERBASE_H
#define MASTER_FRAMEWORK_MASTERBASE_H

#include "master_framework/common.h"
#include "worker_framework/LeaderedWorkerBase.h"
#include "hippo/MasterDriver.h"

BEGIN_MASTER_FRAMEWORK_NAMESPACE(master_base);

class MasterBase : public worker_framework::LeaderedWorkerBase {
public:
    MasterBase();
    virtual ~MasterBase();

private:    
    MasterBase(const MasterBase &);
    MasterBase& operator=(const MasterBase &);

protected:
    virtual void doInitOptions();
    virtual void doExtractOptions();
    virtual bool doInit();
    virtual bool doStart();
    virtual bool doPrepareToRun();
    virtual void doIdle() {};
    virtual void doStop() {};
    virtual void becomeLeader() {}
    virtual void noLongerLeader() {}

public:
    std::string getZkPath() { return _zkPath;}
    
    std::string getHippoZkPath() { return _hippoZkPath;}
    
    std::string getAppId() { return _appId;}

    std::string getMasterAddr() { return _masterAddr; }

public: // for test
    void setZkPath(const std::string &zkPath) { _zkPath = zkPath;}
    
private:
    std::string _zkPath;
    std::string _hippoZkPath;
    std::string _masterAddr;
    std::string _appId;
};

MASTER_FRAMEWORK_TYPEDEF_PTR(MasterBase);

END_MASTER_FRAMEWORK_NAMESPACE(master_base);

#endif //MASTER_FRAMEWORK_MASTERBASE_H
