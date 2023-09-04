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
#ifndef HIPPO_BASICMASTERDRIVER_H
#define HIPPO_BASICMASTERDRIVER_H

#include "util/Log.h"
#include "common/common.h"
#include "autil/LoopThread.h"
#include "autil/Lock.h"
#include "hippo/MasterDriver.h"
#include "AmLeaderChecker.h"
#include "sdk/SlotAllocatorBase.h"
#include "sdk/ProcessLauncherBase.h"

BEGIN_HIPPO_NAMESPACE(sdk);

#define FUNCTIONARRAYSIZE (sizeof(hippo::MasterDriverEvent) * 8)

typedef std::function<
    void (MasterDriverEvent events,
          const std::vector<hippo::SlotInfo*>*,
          std::string)> TriggerFunction;

class BasicMasterDriver : public hippo::MasterDriver {
public:
    const static int64_t SDK_DRIVER_SCHEDULE_INTERVAL_US = 1000 * 1000;
public:
    BasicMasterDriver();
    virtual ~BasicMasterDriver();
private:
    BasicMasterDriver(const BasicMasterDriver &);
    BasicMasterDriver& operator=(const BasicMasterDriver &);
public:
    /* override */ bool init(const std::string &masterZkRoot,
                             const std::string &leaderElectRoot,
                             const std::string &appMasterAddr,
                             const std::string &applicationId);
    /* override */ bool init(
            const std::string &masterZkRoot,
            worker_framework::LeaderChecker * leaderChecker,
            const std::string &applicationId);

    /* override */ bool start();
    /* override */ bool stop();
    /* override */ void detach();
    /* override */ void registerEvent(uint64_t events,
            const hippo::EventFunctionType &function);
    /* override */ void clearEvent(hippo::MasterDriverEvent events);
    /* override */ std::string getApplicationId() const;
    /* override */ void setApplicationId(const std::string &appId);
    /* override */ int64_t getAppChecksum() const;

    void triggerEvent(hippo::MasterDriverEvent events,
                      const std::vector<hippo::SlotInfo*> *slotInfos = NULL,
                      std::string message = "");

    virtual bool isConnected() const;

    virtual bool isWorking() const;

    virtual hippo::MasterInfo getMasterInfo() const;

    virtual void getSlots(std::vector<hippo::SlotInfo> *slots) const;

    virtual void getSlotsByRole(const std::string &role,
            std::vector<hippo::SlotInfo> *slots) const;

    virtual void getReclaimingSlots(
            std::vector<hippo::SlotInfo> *slots) const;

    virtual void updateRoleRequest(const std::string &role,
            const std::vector<SlotResource> &options,
            const std::vector<Resource> &declare,
            const std::string &queue,
            const groupid_t &groupId,
            ResourceAllocateMode allocateMode,
            int32_t count,
            Priority priority = Priority(),
            MetaTagMap metaTags = MetaTagMap(),
            hippo::CpusetMode cpusetMode = hippo::CpusetMode::RESERVED);

    virtual void updateRoleRequest(const std::string &role,
            const RoleRequest &request);

    virtual void releaseSlot(const hippo::SlotId &slotId,
                                    const hippo::ReleasePreference &pref,
                                    int32_t reserveTime = 0);

    virtual void releaseRoleSlots(const std::string &role,
            const hippo::ReleasePreference &pref,
            int32_t reserveTime = 0);

    virtual void setRoleProcess(const std::string &role,
            const hippo::ProcessContext &context,
            const std::string &scope = "");

    virtual void clearRoleProcess(const std::string &role);

    virtual bool updatePackages(const hippo::SlotId &slotId,
            const std::vector<hippo::PackageInfo> &packages);

    virtual bool isPackageReady(const hippo::SlotId &slotId);

    virtual bool updatePreDeployPackages(const SlotId &slotId,
            const std::vector<PackageInfo> &packages);

    virtual bool isPreDeployPackageReady(const hippo::SlotId &slotId);


    virtual bool updateDatas(const hippo::SlotId &slotId,
                                    const std::vector<hippo::DataInfo> &datas,
                                    bool force = false);

    virtual bool isDataReady(
            const hippo::SlotId &slotId,
            const std::vector<std::string> &dataNames);

    virtual bool restartProcess(const hippo::SlotId &slotId,
            const std::vector<std::string> &processNames);

    virtual bool isProcessReady(const hippo::SlotId &slotId,
            const std::vector<std::string> &processNames);

    virtual std::string getProcessWorkDir(const std::string &procName) const = 0;
    virtual std::string getProcessWorkDir(const std::string &workDirTag,
            const std::string &procName) const  = 0;

    virtual LeaderSerializer* createLeaderSerializer(
            const std::string &zookeeperRoot, const std::string &fileName,
            const std::string &backupRoot = "");

    virtual bool resetSlot(const hippo::SlotId &slotId);

    virtual void setSlotProcess(
            const std::vector<SlotInfo> &slotVec,
            const ProcessContext &context,
            const std::string &scope = "");

    virtual void setSlotProcess(
            const std::vector<std::pair<std::vector<SlotInfo>, hippo::ProcessContext> > &slotLaunchPlans,
            const std::string &scope = "");

    virtual void getRoleNames(std::set<std::string> *roleNames) const;

    virtual hippo::ErrorInfo getLastErrorInfo() const;

public:
    void workLoop();

public: //for test
    bool isLeader() const {
        return _isLeader;
    }

    void setLeaderChecker(worker_framework::LeaderChecker *leaderChecker) {
        if (!_amLeaderChecker) {
            _amLeaderChecker = new AmLeaderChecker();
        }
        _amLeaderChecker->setLeaderChecker(leaderChecker);
    }

    void setAmLeaderCheckerCreator(
            AmLeaderCheckerCreator *amLeaderCheckerCreator)
    {
        delete _amLeaderCheckerCreator;
        _amLeaderCheckerCreator = amLeaderCheckerCreator;
    }

    MasterInfo &getMasterInfo() {
        return _masterInfo;
    }

protected:
    virtual bool initSlotAllocator(const std::string &masterZkRoot,
                                   const std::string &applicationId) = 0;
    virtual bool initPorcessLauncher(const std::string &masterZkRoot,
            const std::string &applicationId) = 0;
    virtual bool initAmLeaderChecker(const std::string &leaderElectRoot,
            const std::string &appMasterAddr);
    virtual bool initAmLeaderChecker(worker_framework::LeaderChecker * leaderChecker);

    //prepare resource before allocate
    virtual void beforeLoop() {}

    //assign allocated resource
    virtual void afterLoop() {}

    bool checkLeader();

protected:
    std::string _appId;
    typedef std::map<uint64_t, hippo::EventFunctionType> FuncMapType;
    FuncMapType _functions;
    mutable autil::ThreadMutex _mutex;

    bool _isLeader;
    bool _isWorking;
    hippo::MasterInfo _masterInfo;
    SlotAllocatorBase *_slotAllocator;
    ProcessLauncherBase *_processLauncher;
    AmLeaderChecker *_amLeaderChecker;
    AmLeaderCheckerCreator *_amLeaderCheckerCreator;
    EventTrigger *_eventTrigger;

private:
    autil::LoopThreadPtr _workingThreadPtr;
private:
    HIPPO_LOG_DECLARE();
};

HIPPO_TYPEDEF_PTR(BasicMasterDriver);

END_HIPPO_NAMESPACE(sdk);

#endif //HIPPO_BASICMASTERDRIVER_H
