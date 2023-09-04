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
#include "aios/apps/facility/cm2/cm_sub/cm_sub_base.h"

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "autil/TimeUtility.h"

namespace cm_sub {

int32_t CMSubscriberBase::reinitLocal(const std::vector<cm_basic::CMCluster*>& vec_cluster)
{
    cm_basic::SubRespMsg resp;
    resp.set_status(cm_basic::SubRespMsg::SRS_SUCCESS);
    resp.set_compress_type(cm_basic::CT_NONE);

    // if 0, clusters can't be added; if use current time, the server clusters can't be synchronized.
    int64_t ver = 1;
    for (size_t i = 0; i < vec_cluster.size(); ++i) {
        if (vec_cluster[i] == NULL) {
            return -1;
        }
        cm_basic::ClusterUpdateInfo update_info;
        update_info.set_msg_type(cm_basic::ClusterUpdateInfo::MT_CLUSTER_REINIT);
        update_info.set_cluster_name(vec_cluster[i]->cluster_name());
        update_info.set_cluster_version(ver);
        vec_cluster[i]->set_version(ver);
        update_info.mutable_cm_cluster()->CopyFrom(*(vec_cluster[i]));
        std::string str_update_info;
        update_info.SerializeToString(&str_update_info);
        resp.add_update_info_vec(str_update_info);
    }
    return updateAllClusterInfo(&resp);
}
} // namespace cm_sub
