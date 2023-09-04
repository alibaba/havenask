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
#include "aios/apps/facility/cm2/cm_basic/leader_election/master_apply.h"

namespace cm_basic {

AUTIL_LOG_SETUP(cm_basic, MasterApply);

int MasterApply::getMaster(const std::string& psz_path, std::string& rs_spec)
{
    if (_zkWrapper.open() == false) {
        AUTIL_LOG(WARN, "return state(%d), connecting zookeeper failed !!", _zkWrapper.getStatus());
        return -1;
    }
    std::vector<std::string> vec_nodes;
    if (_zkWrapper.getChild(psz_path, vec_nodes) != ZOK) {
        AUTIL_LOG(WARN, "get child of %s failed", psz_path.c_str());
        _zkWrapper.close();
        return -1;
    }
    for (size_t i = 0; i < vec_nodes.size(); ++i) {
        bool b_primary = false;
        if (_zkWrapper.leaderElectorStrategy(vec_nodes, psz_path, vec_nodes[i], b_primary) < 0) {
            AUTIL_LOG(WARN, "get leader from %s:%s failed", psz_path.c_str(), vec_nodes[i].c_str());
            _zkWrapper.close();
            return -1;
        }
        if (b_primary) {
            if (ZOK == _zkWrapper.getData(psz_path + '/' + vec_nodes[i], rs_spec)) {
                _zkWrapper.close();
                return 0;
            }
        }
    }
    AUTIL_LOG(WARN, "get master failed.basepath(%s) nodenum(%d)", psz_path.c_str(), (int)(vec_nodes.size()));
    _zkWrapper.close();
    return -1;
}

} // namespace cm_basic
