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
#ifndef CARBON_REPLICANODECOMP_H
#define CARBON_REPLICANODECOMP_H

#include "common/common.h"
#include "master/ReplicaNode.h"

BEGIN_CARBON_NAMESPACE(master);

class ReplicaNodeScorer {
public:
    static int32_t score(const ReplicaNodePtr &nodePtr);
};

class ReplicaNodeComp
{
public:
    bool operator () (const ReplicaNodePtr &a, const ReplicaNodePtr &b) {
        int32_t scoreA = ReplicaNodeScorer::score(a);
        int32_t scoreB = ReplicaNodeScorer::score(b);
        if (scoreA == scoreB) {
            return a->getId() < b->getId();
        }
        
        return scoreA < scoreB;
    }
};

CARBON_TYPEDEF_PTR(ReplicaNodeComp);

END_CARBON_NAMESPACE(master);

#endif //CARBON_REPLICANODECOMP_H
