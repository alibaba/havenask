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
#ifndef CARBON_REPLICANODE_H
#define CARBON_REPLICANODE_H

/*
 * =============================================================
 * Several Principle of ReplicaNode
 * 1. represent for logical node of upper layer
 * 2. never release self unless upper layer choose it to release
 * 3. always has current workernode
 * =============================================================
 */

#include "common/common.h"
#include "carbon/Log.h"
#include "master/WorkerNode.h"
#include "master/BrokenRecoverQuota.h"
#include "master/WorkerNodeCreator.h"
#include "master/ExtVersionedPlan.h"

BEGIN_CARBON_NAMESPACE(master);

class ReplicaNode : public autil::legacy::Jsonizable
{
public:
    ReplicaNode();
    
    ReplicaNode(const nodeid_t &nodeId,
                const BrokenRecoverQuotaPtr &quotaPtr,
                const WorkerNodeCreatorPtr &workerNodeCreatorPtr = WorkerNodeCreatorPtr());
    virtual ~ReplicaNode();
public:
    ReplicaNode(const ReplicaNode &);
    ReplicaNode& operator=(const ReplicaNode &);
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    
public:
    void setPlan(const version_t &version, const ExtVersionedPlan &plan);

    void setFinalPlan(const version_t &version, const ExtVersionedPlan &extPlan);
    
    void schedule();

    /* virtual for mock */
    virtual void release();

    /* virtual for mock */
    virtual bool isReleased() const;

    bool isReleasing() const { return _releasing; }

    void getWorkerNodes(WorkerNodeVect *workerNodes) const;

    version_t getVersion() const { return _version; }

    nodeid_t getId() const {
        return _nodeId;
    }

    /* virtual for mock */
    virtual bool isAvailable() const; //use to cal available matrix when rolling

    /* virtual for mock */
    virtual bool isCompleted() const; //the replica node is ready for target
    
    bool targetHasReached() const;

    bool isUnAssignedSlot() const;

    void fillReplicaNodeStatus(ReplicaNodeStatus *status) const;

    bool recover(const version_t &latestVersion,
                 const ExtVersionedPlanMap &versionedPlans,
                 const BrokenRecoverQuotaPtr &brokenRecoverQuotaPtr,
                 const WorkerNodeCreatorPtr &workerNodeCreatorPtr);

    void setTimeStamp(int64_t timeStamp) {
        _timeStamp = timeStamp;
    }

    void setSmoothRecover(bool flag) {
        _smoothRecover = flag;
    }
    
public: /* for test */
    WorkerNodePtr getCurWorkerNode() const {
        return _curWorkerNodePtr;
    }

    WorkerNodePtr getBackupWorkerNode() const {
        return _backupWorkerNodePtr;
    }

private:
    void adjustInternalNode();

    void scheduleNodes();

    void releaseCurNodeAndSwapBackup();
private:
    nodeid_t _nodeId;
    WorkerNodePtr _curWorkerNodePtr;
    WorkerNodePtr _backupWorkerNodePtr;
    version_t _version;
    ExtVersionedPlan _extPlan;
    bool _releasing;
    int64_t _timeStamp;
    BrokenRecoverQuotaPtr _quotaPtr;
    WorkerNodeCreatorPtr _workerNodeCreatorPtr;
    bool _smoothRecover;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ReplicaNode);

typedef std::vector<ReplicaNodePtr> ReplicaNodeVect;

END_CARBON_NAMESPACE(master);

#endif //CARBON_REPLICANODE_H
