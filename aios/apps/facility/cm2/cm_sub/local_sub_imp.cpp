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
#include "aios/apps/facility/cm2/cm_sub/local_sub_imp.h"

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "aios/apps/facility/cm2/cm_basic/config/config_manager.h"
#include "aios/apps/facility/cm2/cm_sub/config/subscriber_config.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_manager.h"

namespace cm_sub {

AUTIL_LOG_SETUP(cm_sub, LocalSubscriberImp);

char* LocalSubscriberImp::getDataFromFile(const char* p_file_name)
{
    FILE* fp = fopen(p_file_name, "r");
    if (NULL == fp) {
        AUTIL_LOG(WARN, "open file(%s) failed.", p_file_name);
        return NULL;
    }
    fseek(fp, 0L, SEEK_END);
    int n_file_len = ftell(fp);
    if (n_file_len <= 0) {
        AUTIL_LOG(WARN, "file length is wrong.%d", n_file_len);
        fclose(fp);
        return NULL;
    }
    fseek(fp, 0L, SEEK_SET);
    char* p_file_buff = new char[n_file_len + 1];
    if (NULL == p_file_buff) {
        AUTIL_LOG(WARN, "getDataFromFile, alloc buffer failed!");
        fclose(fp);
        return NULL;
    }
    int32_t n_read_filelen = fread(p_file_buff, 1, n_file_len, fp);
    p_file_buff[n_read_filelen] = '\0';
    fclose(fp);
    return p_file_buff;
}

int32_t LocalSubscriberImp::init(TopoClusterManager* topo_cluster, cm_basic::CMCentralSub* cm_central)
{
    setTopoClusterManager(topo_cluster);
    setCMCentral(cm_central);

    cm_basic::ClustermapConfig config_manager;
    char* psz_file_data = getDataFromFile(_cfgInfo->_clusterCfg);
    if (unlikely(NULL == psz_file_data)) {
        return -1;
    }
    // NCT_KEEPONLINE will set all nodes NORMAL
    if (config_manager.parse(psz_file_data, cm_basic::CMCluster::NCT_KEEP_ONLINE) != 0) {
        delete[] psz_file_data;
        return -1;
    }
    delete[] psz_file_data;

    std::vector<cm_basic::CMCluster*>& vec_cluster = config_manager.getAllCluster();
    return reinitLocal(vec_cluster);
}

} // namespace cm_sub
