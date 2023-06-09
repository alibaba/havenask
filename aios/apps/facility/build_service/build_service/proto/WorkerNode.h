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
#ifndef ISEARCH_BS_WORKERNODE_H
#define ISEARCH_BS_WORKERNODE_H

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "hippo/proto/Common.pb.h"

namespace build_service { namespace proto {

template <RoleType ROLE_TYPE>
struct RoleType2PartitionInfoTypeTraits {
    typedef void CurrentStatusType;
    typedef void TargetStatusType;
    typedef void PartitionInfoType;
};

template <typename NODES>
struct WorkerNodes2WorkerTypeTraits {
    typedef void WorkerType;
};

#define WORKERNODES_2_WORKER_TYPE(w, wt, rt)                                                                           \
    template <>                                                                                                        \
    struct WorkerNodes2WorkerTypeTraits<w> {                                                                           \
        typedef wt WorkerType;                                                                                         \
        static const RoleType WorkerRoleType = rt;                                                                     \
    }

#define ROLETYPE_2_PARTITIONINFO_HELPER(r, p, c, t)                                                                    \
    template <>                                                                                                        \
    struct RoleType2PartitionInfoTypeTraits<r> {                                                                       \
        typedef c CurrentStatusType;                                                                                   \
        typedef t TargetStatusType;                                                                                    \
        typedef p PartitionInfoType;                                                                                   \
    }

ROLETYPE_2_PARTITIONINFO_HELPER(ROLE_TASK, TaskPartitionInfo, TaskCurrent, TaskTarget);
ROLETYPE_2_PARTITIONINFO_HELPER(ROLE_PROCESSOR, ProcessorPartitionInfo, ProcessorCurrent, ProcessorTarget);
ROLETYPE_2_PARTITIONINFO_HELPER(ROLE_BUILDER, BuilderPartitionInfo, BuilderCurrent, BuilderTarget);
ROLETYPE_2_PARTITIONINFO_HELPER(ROLE_MERGER, MergerPartitionInfo, MergerCurrent, MergerTarget);
ROLETYPE_2_PARTITIONINFO_HELPER(ROLE_JOB, JobWorkerPartitionInfo, JobCurrent, JobTarget);
ROLETYPE_2_PARTITIONINFO_HELPER(ROLE_AGENT, AgentPartitionInfo, AgentCurrent, AgentTarget);

#define COMPARE_PB_FIELD1(field1) l.field1() == r.field1()

#define COMPARE_PB_FIELD2(field1, field2) l.field1() == r.field1() && l.field2() == r.field2()

#define COMPARE_PB_FIELD3(field1, field2, field3)                                                                      \
    l.field1() == r.field1() && l.field2() == r.field2() && l.field3() == r.field3()

#define COMPARE_FIELD1(name, field1)                                                                                   \
    static bool equalStatus(const name& l, const name& r) { return COMPARE_PB_FIELD1(field1); }

#define COMPARE_FIELD2(name, field1, field2)                                                                           \
    static bool equalStatus(const name& l, const name& r) { return COMPARE_PB_FIELD2(field1, field2); }

#define COMPARE_FIELD3(name, field1, field2, field3)                                                                   \
    static bool equalStatus(const name& l, const name& r) { return COMPARE_PB_FIELD3(field1, field2, field3); }

#define COMPARE_FIELD4(name, field1, field2, field3, field4)                                                           \
    static bool equalStatus(const name& l, const name& r)                                                              \
    {                                                                                                                  \
        return COMPARE_PB_FIELD2(field1, field2) && COMPARE_PB_FIELD2(field3, field4);                                 \
    }

#define COMPARE_FIELD5(name, field1, field2, field3, field4, field5)                                                   \
    static bool equalStatus(const name& l, const name& r)                                                              \
    {                                                                                                                  \
        return COMPARE_PB_FIELD2(field1, field2) && COMPARE_PB_FIELD3(field3, field4, field5);                         \
    }

#define COMPARE_FIELD6(name, field1, field2, field3, field4, field5, field6)                                           \
    static bool equalStatus(const name& l, const name& r)                                                              \
    {                                                                                                                  \
        return COMPARE_PB_FIELD3(field1, field2, field3) && COMPARE_PB_FIELD3(field4, field5, field6);                 \
    }

#define COMPARE_FIELD7(name, field1, field2, field3, field4, field5, field6, field7)                                   \
    static bool equalStatus(const name& l, const name& r)                                                              \
    {                                                                                                                  \
        return COMPARE_PB_FIELD3(field1, field2, field3) && COMPARE_PB_FIELD3(field4, field5, field6) &&               \
               COMPARE_PB_FIELD1(field7);                                                                              \
    }

#define ADD_TO_HISTORY(reason)                                                                                         \
    do {                                                                                                               \
        auto workerHistory = buildHistory(info, reason);                                                               \
        addHistory(workerHistory);                                                                                     \
        return;                                                                                                        \
    } while (0)

bool operator==(const hippo::proto::SlotId& l, const hippo::proto::SlotId& r);
bool operator!=(const hippo::proto::SlotId& l, const hippo::proto::SlotId& r);
bool operator==(const hippo::proto::AssignedSlot& l, const hippo::proto::AssignedSlot& r);
bool operator!=(const hippo::proto::AssignedSlot& l, const hippo::proto::AssignedSlot& r);

class WorkerNodeBase
{
public:
    WorkerNodeBase(const PartitionId& partitionId)
        : _partitionId(partitionId)
        , _finished(false)
        , _suspended(false)
        , _keepAliveAfterFinish(false)
        , _exceedReleaseLimit(false)
        , _isHighQuality(false)
        , _isReady(true)
    {
    }
    virtual ~WorkerNodeBase() {}
    typedef ::google::protobuf::RepeatedPtrField<hippo::proto::AssignedSlot> PBSlotInfos;
    struct ResourceInfo : public autil::legacy::Jsonizable {
    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
        {
            json.Jsonize("resource_id", resourceId, resourceId);
            json.Jsonize("resource_str", resourceStr, resourceStr);
        }
        bool operator==(const ResourceInfo& other)
        {
            return resourceStr == other.resourceStr && resourceId == other.resourceId;
        }
        bool operator!=(const ResourceInfo& other) { return !(operator==(other)); }

    public:
        std::string resourceStr;
        std::string resourceId;
    };

public:
    const PartitionId& getPartitionId() const { return _partitionId; }
    virtual RoleType getRoleType() const = 0;
    virtual std::string getTargetConfigPath() const = 0;
    virtual bool isTargetSuspending() const = 0;
    virtual int32_t getWorkerProtocolVersion() const = 0;
    virtual std::string getWorkerConfigPath() const = 0;
    virtual void setTargetStatusStr(const std::string& targetStatus) = 0;
    virtual void getTargetStatusStr(std::string& target, std::string& requiredIdentifier) const = 0;
    virtual void setCurrentStatusStr(const std::string& currentStatus, const std::string& currentIdentifier) = 0;
    virtual std::string getCurrentStatusStr() const = 0;
    virtual void setSlotInfo(const PBSlotInfos& slotInfos) = 0;
    virtual void changeSlots() = 0;
    virtual bool getToReleaseSlots(PBSlotInfos& toReleaseSlotInfos) { return false; }

    /*
        below function only mark a worker is finished,
        do not promise a node is realy finished / suspended.
    */
    bool isFinished() const { return _finished; }
    void setFinished(bool isFinished) { _finished = isFinished; }

    bool isReady() const { return _isReady; }
    void setIsReady(bool isReady) { _isReady = isReady; }

    bool isSuspended() const { return _suspended; }
    void setSuspended(bool isSuspended) { _suspended = isSuspended; }

    /*
        optimization for tiny table of igraph, do not reallocate hippo role during the whole procedure.
    */
    void setKeepAliveAfterFinish(bool keepAliveAfterFinish) { _keepAliveAfterFinish = keepAliveAfterFinish; }
    bool keepAliveAfterFinish() const { return _keepAliveAfterFinish; }

    bool exceedReleaseLimit() const { return _exceedReleaseLimit; }
    void setExceedReleaseLimit() { _exceedReleaseLimit = true; }
    void resetExceedReleaseLimit() { _exceedReleaseLimit = false; }

    void setHighQuality(bool isHighQuality) { _isHighQuality = isHighQuality; }
    bool isHighQuality() const { return _isHighQuality; }
    void setNodeId(uint32_t nodeId) { _nodeId = nodeId; }
    uint32_t getNodeId() { return _nodeId; }
    void setInstanceIdx(uint32_t instanceIdx) { _instanceIdx = instanceIdx; }
    uint32_t getInstanceIdx() { return _instanceIdx; }
    void setSourceNodeId(uint32_t sourceNodeId) { _sourceNodeId = sourceNodeId; }
    uint32_t getSourceNodeId() const { return _sourceNodeId; }
    uint32_t getBackupId() const
    {
        if (_partitionId.has_backupid()) {
            return _partitionId.backupid();
        } else {
            return -1;
        }
    }

    bool getUsingResource(const std::string& resourceName, ResourceInfo*& resourceInfo)
    {
        autil::ScopedLock lock(_resourceInfoLock);
        resourceInfo = NULL;
        auto iter = _usingResources.find(resourceName);
        if (iter != _usingResources.end()) {
            resourceInfo = &(iter->second);
            return true;
        }
        return false;
    }

    void addUsingResource(const std::string& name, const std::string& resourceStr, const std::string& resourceId)
    {
        autil::ScopedLock lock(_resourceInfoLock);
        addUsingResourceUnSafe(name, resourceStr, resourceId);
    }

    void addTargetDependResource(const std::string& name, const std::string& resourceStr, const std::string& resourceId)
    {
        autil::ScopedLock lock(_resourceInfoLock);
        addTargetDependResourceUnSafe(name, resourceStr, resourceId);
    }

    bool getTargetResource(const std::string& resourceName, ResourceInfo*& resourceInfo)
    {
        autil::ScopedLock lock(_resourceInfoLock);
        auto iter = _targetResources.find(resourceName);
        if (iter != _targetResources.end()) {
            resourceInfo = &(iter->second);
            return true;
        }
        return false;
    }

    void fillTargetDependResources(std::string& dependResourcesStr)
    {
        // _tarResources -> targetResources
        autil::ScopedLock lock(_resourceInfoLock);
        dependResourcesStr = ToJsonString(_targetResources);
    }

    void setTargetDependResources(const std::string& targetResourcesStr)
    {
        // targetResource -> _targetResources
        if (targetResourcesStr.empty()) {
            return;
        }
        autil::ScopedLock lock(_resourceInfoLock);
        _targetResources.clear();
        try {
            FromJsonString(_targetResources, targetResourcesStr);
        } catch (const autil::legacy::ExceptionBase& e) {
            assert(false);
        }
    }

    void getCurrentResources(std::string& usingResources)
    {
        // _usingResources -> usingResource
        autil::ScopedLock lock(_resourceInfoLock);
        usingResources = ToJsonString(_usingResources);
    }

    void setCurrentResources(const std::string& usingResources)
    {
        // usingResources -> _usingResources
        if (usingResources.empty()) {
            return;
        }
        autil::ScopedLock lock(_resourceInfoLock);
        _usingResources.clear();
        try {
            FromJsonString(_usingResources, usingResources);
        } catch (const autil::legacy::ExceptionBase& e) {
            assert(false);
        }
    }

    static std::string getIdentifier(const std::string& ip, int32_t slotId)
    {
        if (ip.empty() || slotId == 0) {
            return "";
        }
        return ip + "|" + autil::legacy::ToString(slotId);
    }

    static std::pair<std::string, std::string> getIpAndSlotId(const std::string& identifier)
    {
        if (identifier.empty()) {
            return {"", ""};
        }
        auto strs = autil::StringUtil::split(identifier, "|");
        assert(strs.size() == 2);
        return {strs[0], strs[1]};
    }

private:
    void addUsingResourceUnSafe(const std::string& name, const std::string& resourceStr, const std::string& resourceId)
    {
        ResourceInfo info;
        info.resourceId = resourceId;
        info.resourceStr = resourceStr;
        _usingResources[name] = info;
    }

    void addTargetDependResourceUnSafe(const std::string& name, const std::string& resourceStr,
                                       const std::string& resourceId)
    {
        ResourceInfo info;
        info.resourceId = resourceId;
        info.resourceStr = resourceStr;
        _targetResources[name] = info;
    }

protected:
    const PartitionId _partitionId;
    volatile bool _finished;
    volatile bool _suspended;
    volatile bool _keepAliveAfterFinish;
    mutable autil::ThreadMutex _resourceInfoLock;
    bool _exceedReleaseLimit;
    bool _isHighQuality;
    bool _isReady;
    std::string _requiredIdentifier;
    std::string _currentIdentifier;
    std::map<std::string, ResourceInfo> _targetResources;
    std::map<std::string, ResourceInfo> _usingResources;
    uint32_t _nodeId = 0;
    uint32_t _instanceIdx = 0;
    uint32_t _sourceNodeId = -1; // if backup node exist, sourceNodeId is mainNodeId
};

BS_TYPEDEF_PTR(WorkerNodeBase);

template <RoleType ROLE_TYPE>
class WorkerNode : public WorkerNodeBase
{
public:
    typedef typename RoleType2PartitionInfoTypeTraits<ROLE_TYPE>::CurrentStatusType CurrentStatusType;
    typedef typename RoleType2PartitionInfoTypeTraits<ROLE_TYPE>::TargetStatusType TargetStatusType;
    typedef typename RoleType2PartitionInfoTypeTraits<ROLE_TYPE>::PartitionInfoType PartitionInfoType;

public:
    WorkerNode(const PartitionId& partitionId) : WorkerNodeBase(partitionId)
    {
        *_partitionInfo.mutable_pid() = partitionId;
    }
    ~WorkerNode() {}

private:
    WorkerNode(const WorkerNode&);
    WorkerNode& operator=(const WorkerNode&);

public: // for worker/admin heartbeat
    RoleType getRoleType() const override { return ROLE_TYPE; }
    void setTargetStatusStr(const std::string& targetStatus) override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        setStatusStr(targetStatus, *_partitionInfo.mutable_targetstatus());
    }
    void getTargetStatusStr(std::string& target, std::string& requiredIdentifier) const override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        requiredIdentifier = _requiredIdentifier;
        target = getStatusStr(_partitionInfo.targetstatus());
    }
    void setCurrentStatusStr(const std::string& currentStatus, const std::string& currentIdentifier) override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        setStatusStr(currentStatus, *_partitionInfo.mutable_currentstatus());
        _currentIdentifier = currentIdentifier;
    }
    std::string getCurrentStatusStr() const override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        return getStatusStr(_partitionInfo.currentstatus());
    }

    void setSlotInfo(const PBSlotInfos& slotInfos) override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        _partitionInfo.mutable_slotinfos()->CopyFrom(slotInfos);
    }

public: // for admin access
    void setTargetStatus(const TargetStatusType& status) { setTargetStatus(status, ""); }
    void setTargetStatus(const TargetStatusType& status, const std::string& requiredIdentifier)
    {
        autil::ScopedLock lock(_partitionInfoLock);
        *_partitionInfo.mutable_targetstatus() = status;
        _requiredIdentifier = requiredIdentifier;
    }
    TargetStatusType getTargetStatus() const
    {
        autil::ScopedLock lock(_partitionInfoLock);
        return _partitionInfo.targetstatus();
    }
    CurrentStatusType getCurrentStatus() const
    {
        autil::ScopedLock lock(_partitionInfoLock);
        return _partitionInfo.currentstatus();
    }
    std::pair<CurrentStatusType, std::string> getCurrentStatusAndIdentifier() const
    {
        autil::ScopedLock lock(_partitionInfoLock);
        return {_partitionInfo.currentstatus(), _currentIdentifier};
    }

    virtual int32_t getWorkerProtocolVersion() const override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        if (!_partitionInfo.currentstatus().has_protocolversion()) {
            return UNKNOWN_WORKER_PROTOCOL_VERSION;
        }
        return _partitionInfo.currentstatus().protocolversion();
    }
    virtual std::string getWorkerConfigPath() const override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        if (!_partitionInfo.currentstatus().has_configpath()) {
            return "";
        }
        return _partitionInfo.currentstatus().configpath();
    }

    virtual bool isTargetSuspending() const override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        return _partitionInfo.targetstatus().suspendtask();
    }

    virtual std::string getTargetConfigPath() const override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        if (!_partitionInfo.targetstatus().has_configpath()) {
            return "";
        }
        return _partitionInfo.targetstatus().configpath();
    }

    PartitionInfoType getPartitionInfo() const
    {
        autil::ScopedLock lock(_partitionInfoLock);
        return _partitionInfo;
    }

    void changeSlots() override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        _toReleaseSlotInfos.Clear();
        _toReleaseSlotInfos.CopyFrom(_partitionInfo.slotinfos());
    }

    bool getToReleaseSlots(PBSlotInfos& toReleaseSlotInfos) override
    {
        autil::ScopedLock lock(_partitionInfoLock);
        for (const auto& slotInfo : _toReleaseSlotInfos) {
            for (const auto& currentSlotInfo : _partitionInfo.slotinfos()) {
                if (slotInfo.id() == currentSlotInfo.id()) {
                    toReleaseSlotInfos.Clear();
                    toReleaseSlotInfos.CopyFrom(_toReleaseSlotInfos);
                    return true;
                }
            }
        }
        return false;
    }

    // for test
    void setCurrentStatus(const CurrentStatusType& status, const std::string& identifier = "")
    {
        autil::ScopedLock lock(_partitionInfoLock);
        *_partitionInfo.mutable_currentstatus() = status;
        _currentIdentifier = identifier;
    }

private:
    template <class T>
    void setStatusStr(const std::string& content, T& typedStatus)
    {
        auto oldTypedStatus = typedStatus;
        typedStatus.Clear();
        if (content.empty()) {
            return;
        }
        bool ret = typedStatus.ParseFromString(content);
        assert(ret);
        (void)ret;
    }
    template <class T>
    static std::string getStatusStr(const T& typedStatus)
    {
        std::string content;
        typedStatus.SerializeToString(&content);
        return content;
    }

protected:
    mutable autil::ThreadMutex _partitionInfoLock;
    PartitionInfoType _partitionInfo;
    PBSlotInfos _toReleaseSlotInfos;

private:
    friend class GenerationTaskStateMachine;
};

typedef std::vector<WorkerNodeBasePtr> WorkerNodes;

typedef WorkerNode<ROLE_PROCESSOR> ProcessorNode;
BS_TYPEDEF_PTR(ProcessorNode);
typedef std::vector<ProcessorNodePtr> ProcessorNodes;
WORKERNODES_2_WORKER_TYPE(ProcessorNodes, ProcessorNode, ROLE_PROCESSOR);

typedef WorkerNode<ROLE_BUILDER> BuilderNode;
BS_TYPEDEF_PTR(BuilderNode);
typedef std::vector<BuilderNodePtr> BuilderNodes;
WORKERNODES_2_WORKER_TYPE(BuilderNodes, BuilderNode, ROLE_BUILDER);

typedef WorkerNode<ROLE_MERGER> MergerNode;
BS_TYPEDEF_PTR(MergerNode);
typedef std::vector<MergerNodePtr> MergerNodes;
WORKERNODES_2_WORKER_TYPE(MergerNodes, MergerNode, ROLE_MERGER);

typedef WorkerNode<ROLE_JOB> JobNode;
BS_TYPEDEF_PTR(JobNode);
typedef std::vector<JobNodePtr> JobNodes;
WORKERNODES_2_WORKER_TYPE(JobNodes, JobNode, ROLE_JOB);

typedef WorkerNode<ROLE_TASK> TaskNode;
BS_TYPEDEF_PTR(TaskNode);
typedef std::vector<TaskNodePtr> TaskNodes;
WORKERNODES_2_WORKER_TYPE(TaskNodes, TaskNode, ROLE_TASK);

typedef WorkerNode<ROLE_AGENT> AgentNode;
BS_TYPEDEF_PTR(AgentNode);
typedef std::vector<AgentNodePtr> AgentNodes;
WORKERNODES_2_WORKER_TYPE(AgentNodes, AgentNode, ROLE_AGENT);

typedef std::map<std::string, JobNodes> JobNodesMap;
typedef std::map<std::string, WorkerNodes> WorkerNodesMap;

}} // namespace build_service::proto

#endif // ISEARCH_BS_WORKERNODE_H
