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
#include "master/ReplicaNodeCreator.h"
#include "carbon/Log.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, ReplicaNodeCreator);

ReplicaNodeCreator::ReplicaNodeCreator(
        const string &baseId,
        const BrokenRecoverQuotaPtr &brokenRecoverQuotaPtr,
        const IdGeneratorPtr &idGeneratorPtr)
{
    _baseId = baseId;
    _brokenRecoverQuotaPtr = brokenRecoverQuotaPtr;
    _idGeneratorPtr = idGeneratorPtr;
}

ReplicaNodeCreator::~ReplicaNodeCreator() {
    
}

WorkerNodeCreatorPtr ReplicaNodeCreator::getWorkerNodeCreator(
        const nodeid_t &nodeId) const
{
    WorkerNodeCreatorPtr workerNodeCreatorPtr(
            new WorkerNodeCreator(nodeId, _idGeneratorPtr));
    return workerNodeCreatorPtr;
}

ReplicaNodePtr ReplicaNodeCreator::create() {
    nodeid_t nodeId = _baseId + "." +
                      StringUtil::toString(_idGeneratorPtr->getId());
    
    WorkerNodeCreatorPtr workerNodeCreatorPtr(
            new WorkerNodeCreator(nodeId, _idGeneratorPtr));
    ReplicaNodePtr nodePtr(new ReplicaNode(nodeId,
                    _brokenRecoverQuotaPtr, workerNodeCreatorPtr));
    return nodePtr;
}

END_CARBON_NAMESPACE(master);

