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
#ifndef HIPPO_MASTERDRIVER_H
#define HIPPO_MASTERDRIVER_H

#include "hippo/MasterEvent.h"
#include "hippo/LeaderSerializer.h"
#include "worker_framework/LeaderChecker.h"
namespace hippo {

/**
 * Interface for Application Manager to issue requests to master & slave.
 */
class MasterDriver
{
protected:
    MasterDriver();
public:
    virtual ~MasterDriver();
public:
    static MasterDriver* createDriver(const std::string &driverName);
public:
    // functions for Application Manager framework
    virtual bool init(const std::string &hippoZkRoot,
                      const std::string &leaderElectRoot,
                      const std::string &appMasterAddr,
                      const std::string &applicationId) = 0;

    virtual bool init(const std::string &hippoZkRoot,
                      worker_framework::LeaderChecker * leaderChecker,
                      const std::string &applicationId) = 0;

    /**
     * Start driver thread
     */
    virtual bool start() = 0;

    /**
     * Stop all the slots and processes.
     * Then inform master to stop this application.
     */
    virtual bool stop() = 0;

    /**
     * Stop driver thread without stopping slots and processes.
     * Another AM may startup and take over these assets.
     */
    virtual void detach() = 0;

    /**
     * Create a leader serializer for AppMaster to serialize some crucial
     * imformations, return NULL if error happens. Serialization's success
     * depends on whether AppMaster is leader.
     * note:
     *    1. zookeeper host should be the same with leader election's host
     *    2. you should delete this LeaderSerializer by yourself, and should
     *       delete this before MasterDriver
     */
    virtual LeaderSerializer* createLeaderSerializer(
            const std::string &zookeeperRoot, const std::string &fileName,
            const std::string &backupRoot = "") = 0;

public:
    // function for event notification
    /**
     * Register event callback function to get informed when events
     * happen. Check MasterEvent.h for events descriptions.
     */
    virtual void registerEvent(/*MasterDriverEvent*/uint64_t events,
                               const EventFunctionType &function) = 0;

    virtual void clearEvent(MasterDriverEvent events) = 0;

public:
    // functions for query-style interface
    virtual std::string getApplicationId() const = 0;
    virtual int64_t getAppChecksum() const = 0;
    virtual bool isConnected() const = 0;
    virtual bool isWorking() const = 0;
    virtual MasterInfo getMasterInfo() const = 0;

    /**
     * Get allocated resources.
     * Application manager should check the reclaiming field in SlotInfo. If
     * it is true, application should migrate data and return the slot as
     * soon as possible.
     **/
    virtual void getSlots(std::vector<SlotInfo> *slots) const = 0;
    virtual void getSlotsByRole(const std::string &role,
                                std::vector<SlotInfo> *slots) const = 0;
    virtual void getReclaimingSlots(std::vector<SlotInfo> *slots) const = 0;

    /**
       depricated, will not update anymore
    **/
    virtual void updateRoleRequest(const std::string &role,
                                   const std::vector<SlotResource> &options,
                                   const std::vector<Resource> &declare,
                                   const std::string &queue,
                                   const groupid_t &groupId,
                                   ResourceAllocateMode allocateMode,
                                   int32_t count,
                                   Priority priority = Priority(),
                                   MetaTagMap metaTags = MetaTagMap(),
                                   CpusetMode cpusetMode = CpusetMode::RESERVED) = 0;

    /**
     * Update the requirement for resource.
     * Once for one role. If all the resources for one role is not needed,
     * call releaseRoleSlots to delete it.
     **/
    virtual void updateRoleRequest(const std::string &role,
                                   const RoleRequest &request) = 0;

    /**
     * Return one slot which is not needed.
     * Preference argument is used as a hint to indicate how this app is
     * willing to use this slot in the future.
     * Note pref is only a hint, do not depend on this feature.
     **/
    virtual void releaseSlot(const SlotId &slotId,
                             const ReleasePreference &pref,
                             int32_t reserveTime = 0) = 0;

    virtual void releaseRoleSlots(const std::string &role,
                                  const ReleasePreference &pref,
                                  int32_t reserveTime = 0) = 0;

    /**
     * Set process requirement for a role. SDK will start these process
     * and keep them running.
     **/
    virtual void setRoleProcess(const std::string &role,
                                const ProcessContext &context,
                                const std::string &scope = "") = 0;

    /**
     * Clear process requirement for a role.
     **/
    virtual void clearRoleProcess(const std::string &role) = 0;

    /**
     * Update package for specified slot, packages will be reinstalled
     * and all processes in these slots will be restarted
     **/
    virtual bool updatePackages(const SlotId &slotId,
                                const std::vector<PackageInfo> &packages) = 0;

    /**
     * Check whether packages are ready in given slot
     **/
    virtual bool isPackageReady(const SlotId &slotId) = 0;

    /**
     * Update pre-deploy package for specified slot,
     **/
    virtual bool updatePreDeployPackages(const SlotId &slotId,
            const std::vector<PackageInfo> &packages) = 0;

    /**
     * Check whether pre-deploy packages are ready in given slot
     **/
    virtual bool isPreDeployPackageReady(const SlotId &slotId) = 0;

    /**
     * Scan data versions of data source, return all valid version in *ascending* order
     **/
    // virtual bool getDataVersions(const std::string &dataSrc,
    //                              std::vector<uint32_t> *versions) = 0;

    /**
     * Update datas for specified slot, slave will re-download all changed datas,
     * user can get data status from SlotInfo's dataStatus. If some error occured
     * and slave did not download data successfully, user can retry update with force=true
     **/
    virtual bool updateDatas(const SlotId &slotId,
                             const std::vector<DataInfo> &datas,
                             bool force = false) = 0;

    /**
     * Check whether datas are ready in given slot
     **/
    virtual bool isDataReady(const SlotId &slotId,
                             const std::vector<std::string> &dataNames) = 0;

    /**
     * Restart process in specified slot, after restart,
     * process's instanceid will be refreshed
     * Return false if error occured, e.g. process not exist in slot
     **/
    virtual bool restartProcess(const SlotId &slotId,
                                const std::vector<std::string> &processNames) = 0;

    /**
     * Check whether processes are ready in given slot
     **/
    virtual bool isProcessReady(const SlotId &slotId,
                                const std::vector<std::string> &processNames) = 0;

    /**
     * DEPRECATED. THIS IS ONLY FOR AMs WITH DP1.
     * Get process's absolute work dir.
     **/
    virtual std::string getProcessWorkDir(const std::string &procName) const = 0;
    virtual std::string getProcessWorkDir(const std::string &role,
            const std::string &procName) const = 0;

    /**
     * Reset slot, it will stop the app witch is running on the slot.
     **/
    virtual bool resetSlot(const hippo::SlotId &slotId) = 0;

    /**
     * Set process requirement with scope for a slot. SDK will start these process
     * and keep them running.
     **/
    virtual void setSlotProcess(const std::vector<SlotInfo> &slotVec,
                                const ProcessContext &context,
                                const std::string &scope = "") = 0;
    /**
     * Set group slotid process requirement, this is used for fiber recover,
     * other appMaster should not use this interface.
     **/
    virtual void setSlotProcess(const std::vector<std::pair<std::vector<SlotInfo>, hippo::ProcessContext> > &slotLaunchPlans,
            const std::string &scope = "") = 0;

    /**
     * Set pod requirement for a slot. SDK will start these containers
     * and keep them running.
     **/
    virtual void setSlotPodDesc(
            const std::vector<SlotInfo> &slotVec,
            const std::string &podDesc) = 0;

    virtual void getRoleNames(std::set<std::string> *roleNames) const = 0;

    /**
     * Get last errorinfo, will return last allocate errorinfo
     **/
    virtual hippo::ErrorInfo getLastErrorInfo() const = 0;
};

};

#endif //HIPPO_MASTERDRIVER_H
