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
#ifndef CARBON_REPLICANODECREATOR_H
#define CARBON_REPLICANODECREATOR_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/ReplicaNode.h"

BEGIN_CARBON_NAMESPACE(master);

class ReplicaNodeCreator
{
public:
    ReplicaNodeCreator(const std::string &baseId,
                       const BrokenRecoverQuotaPtr &brokenRecoverQuotaPtr,
                       const IdGeneratorPtr &idGeneratorPtr);
    virtual ~ReplicaNodeCreator();
private:
    ReplicaNodeCreator(const ReplicaNodeCreator &);
    ReplicaNodeCreator& operator=(const ReplicaNodeCreator &);
public:
    virtual ReplicaNodePtr create();

    BrokenRecoverQuotaPtr getBrokenRecoverQuota() const {
        return _brokenRecoverQuotaPtr;
    }
    
    WorkerNodeCreatorPtr getWorkerNodeCreator(const nodeid_t &nodeId) const;

private:
    std::string _baseId;
    BrokenRecoverQuotaPtr _brokenRecoverQuotaPtr;
    IdGeneratorPtr _idGeneratorPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(ReplicaNodeCreator);

END_CARBON_NAMESPACE(master);

#endif //CARBON_REPLICANODECREATOR_H
