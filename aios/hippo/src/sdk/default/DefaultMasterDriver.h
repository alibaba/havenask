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
#ifndef HIPPO_DEFAULTMASTERDRIVER_H
#define HIPPO_DEFAULTMASTERDRIVER_H

#include "util/Log.h"
#include "common/common.h"
#include "sdk/BasicMasterDriver.h"

BEGIN_HIPPO_NAMESPACE(sdk);

class DefaultSlotAllocator;
class DefaultProcessLauncher;
class DefaultSlotAssigner;

class DefaultMasterDriver : public BasicMasterDriver {
public:
    DefaultMasterDriver();
    ~DefaultMasterDriver();
private:
    DefaultMasterDriver(const DefaultMasterDriver &);
    DefaultMasterDriver& operator=(const DefaultMasterDriver &);
public:
    /* override */ bool init(const std::string &masterZkRoot,
                             const std::string &leaderElectRoot,
                             const std::string &appMasterAddr,
                             const std::string &applicationId);

    /* override */ bool init(const std::string &masterZkRoot,
                             worker_framework::LeaderChecker * leaderChecker,
                             const std::string &applicationId);
    /* override */ std::string getProcessWorkDir(const std::string &procName) const;
    /* override */ std::string getProcessWorkDir(const std::string &workDirTag,
            const std::string &procName) const;
    /*override*/ void setSlotPodDesc(const std::vector<SlotInfo> &slotVec,
            const std::string &podDesc) {}
protected:
    /* override */ bool initSlotAllocator(const std::string &masterZkRoot,
            const std::string &applicationId);
    /* override */ bool initPorcessLauncher(const std::string &masterZkRoot,
            const std::string &applicationId);
    virtual void beforeLoop();
    virtual void afterLoop();

private:
    bool createSerializer(const std::string &zookeeperRoot,
                          const std::string &fileName,
                          LeaderSerializer **serializer);
private:
    const static std::string ASSIGNED_SLOTS_FILE;
    const static std::string CANDIDATE_IPS_FILE;
private:
    std::string _homeDir;
    DefaultSlotAssigner* _slotAssigner;
    LeaderSerializer* _assignedSlotsSerializer;
    LeaderSerializer* _ipListReader;
    std::string _ipInfoStr;
    bool _isFirstStart;
    HIPPO_LOG_DECLARE();
};

HIPPO_INTERNAL_TYPEDEF_PTR(DefaultMasterDriver);

END_HIPPO_NAMESPACE(sdk);

#endif  // HIPPO_DEFAULTMASTERDRIVER_H
