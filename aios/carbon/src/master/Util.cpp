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
#include "master/Util.h"
#include "carbon/Log.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, Util);

Util::Util() {
}

Util::~Util() {
}

string Util::getValue(const KVMap &kvMap, const string &key,
                      const string &defaultValue)
{
    KVMap::const_iterator it = kvMap.find(key);
    if (it == kvMap.end()) {
        return defaultValue;
    }

    return it->second;
}

int32_t Util::calcGroupLatestVersionRatio(const GroupPlan &globalPlan) {
    int32_t latestVersionRatio = 100;
    for (map<roleid_t, RolePlan>::const_iterator it = globalPlan.rolePlans.begin();
         it != globalPlan.rolePlans.end(); it++)
    {
        latestVersionRatio = min(latestVersionRatio,
                it->second.global.latestVersionRatio);
    }
    return latestVersionRatio;
}

string Util::workerNodeIdToRoleGUID(const string &workerNodeId) {
    string replicaNodeId = workerNodeId.substr(0, workerNodeId.find_last_of('.'));
    return replicaNodeId.substr(0, replicaNodeId.find_last_of('.'));
}

END_CARBON_NAMESPACE(master);

