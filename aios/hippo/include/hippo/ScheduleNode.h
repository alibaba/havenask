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
#ifndef HIPPO_SCHEDULENODE_H
#define HIPPO_SCHEDULENODE_H

#include "hippo/DriverCommon.h"

namespace hippo {

typedef std::map<std::string, ProcessStatus> ProcessStatusMap;
typedef std::map<std::string, DataStatus> DataStatusMap;

typedef std::map<std::string, PackageInfo> PackageInfos;
typedef std::map<std::string, ProcessInfo> ProcessInfos;
typedef std::map<std::string, DataInfo> DataInfos;

class ScheduleNode
{
protected:
    ScheduleNode(){}
public:
    virtual ~ScheduleNode(){}
private:
    ScheduleNode(const ScheduleNode &);
    ScheduleNode& operator=(const ScheduleNode &);
public:
    // state change: reclaiming=>offline=>release
    // business ask to change this node
    virtual void offline() = 0;
    virtual void online() = 0;    
    virtual bool isOffline() = 0;
    // return to hippo, reset shared_ptr after release
    virtual void release(const ReleasePreference *releasePreference = NULL) = 0;
    // node is releasing, reset shared_ptr
    virtual bool isReleasing() = 0;
    // from hippo runtime info
    virtual bool isReclaiming() = 0;
    virtual bool isReclaimed() = 0;
    virtual bool isWorking() = 0;
    
    // may resource requirement changed, here should offline this node for new one
    virtual bool resourceNolongerMatch() = 0;
    virtual std::string getAddressInfo() = 0;
    virtual SlotId getSlotId() = 0;
    virtual SlaveStatus getSlaveStatus() = 0;
    virtual PackageStatus getPackageStatus() = 0;
    virtual std::string getPackageChecksum() = 0;
    virtual ProcessStatusMap getProcessStatus() = 0;
    virtual DataStatusMap getDataStatus() = 0;
    // if not exist, return false
    virtual bool getProcessStatus(const std::string &name,
                                  ProcessStatus *status) = 0;
    // if not exist, return false
    virtual bool getDataStatus(const std::string &name,
                               DataStatus *status) = 0;
    virtual std::string getIp() = 0;
    // name = slaveAddress + "_" + slotid
    virtual std::string getName() = 0;
    virtual bool isPackageReady() = 0;
    virtual bool isDataReady(const std::string dataName = "") = 0;
    virtual bool isProcessReady(const std::string processName = "") = 0;
    virtual SlotResource getSlotResource() = 0;

    virtual void setPayload(const std::string &key,
                            const std::string &value,
                            bool needSerialize) = 0;
    virtual bool getPayload(
            const std::string &key, std::string &value) = 0;
    
    // business => framework
    virtual void setPlan(const PackageInfos &packageInfos,
                         const DataInfos &dataInfos,
                         const ProcessInfos &processInfos) = 0;
    
    // clear specified plan, and scheduleUnit's plan will take effective
    virtual void clearSpecifiedPlan() = 0;
    virtual void clearPayload() = 0;    
    
    virtual void updatePackageInfos(const PackageInfos &packageInfos) = 0;
    
    virtual void updateProcessInfos(const ProcessInfos &processInfos) = 0;
    
    virtual void updateDataInfos(const DataInfos &dataInfos) = 0;
    
    // business <= framework
    virtual PackageInfos getPackageInfos() = 0;
    virtual ProcessInfos getProcessInfos() = 0;
    virtual DataInfos getDataInfos() = 0;
 
    virtual ProcessContext getContext() = 0;
};

HIPPO_TYPEDEF_PTR(ScheduleNode);

}; //end of hippo namespace

#endif //HIPPO_SCHEDULENODE_H
