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
#include "aios/apps/facility/cm2/cm_sub/cm_sub_imp.h"

#include "aios/apps/facility/cm2/cm_basic/basic_struct/cm_central_sub.h"
#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_subscriber.pb.h"
#include "aios/apps/facility/cm2/cm_basic/config/cmcluster_xml_build.h"
#include "aios/apps/facility/cm2/cm_basic/config/config_manager.h"
#include "aios/apps/facility/cm2/cm_basic/config/config_option.h"
#include "aios/apps/facility/cm2/cm_basic/leader_election/master_server.h"
#include "aios/apps/facility/cm2/cm_basic/util/file_util.h"
#include "aios/apps/facility/cm2/cm_sub/config/subscriber_config.h"
#include "aios/apps/facility/cm2/cm_sub/server_resolver.h"
#include "aios/apps/facility/cm2/cm_sub/service_node.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster.h"
#include "aios/apps/facility/cm2/cm_sub/topo_cluster_manager.h"

namespace cm_sub {

#define SUBSCRIBER_MAJRO_VER 0x0100
#define SUBSCRIBER_MINOR_VER 0x02

using namespace cm_basic;

AUTIL_LOG_SETUP(cm_sub, CMSubscriberImp);

CMSubscriberImp::CMSubscriberImp(SubscriberConfig* cfg_info, arpc::ANetRPCChannelManager* arpc,
                                 IServerResolver* server_resolver)
    : CMSubscriberBase(cfg_info)
    , _arpc(arpc)
    , _serverResolver(server_resolver)
    , _stopFlag(false)
    , _retryMax(2)
    , _expireTime(3 * 1000)
    , _stub(NULL)
    , _reqMsg(NULL)
{
}

CMSubscriberImp::~CMSubscriberImp()
{
    unsubscriber();

    deletePtr(_reqMsg);
    deletePtr(_stub);
}

int32_t CMSubscriberImp::init(TopoClusterManager* topo_cluster, cm_basic::CMCentralSub* cm_central)
{
    setTopoClusterManager(topo_cluster);
    setCMCentral(cm_central);

    // 初始化 订阅请求
    _reqMsg = new cm_basic::SubReqMsg();
    if (_reqMsg == NULL) {
        AUTIL_LOG(ERROR, "cm_sub : CMSubscriberImp::init(), SubReqMsg create failed !!");
        return -1;
    }
    _reqMsg->set_sub_type(_cfgInfo->_subType);
    _reqMsg->set_compress_type(_cfgInfo->_compressType);
    _reqMsg->set_cli_ver(SUBSCRIBER_MAJRO_VER | SUBSCRIBER_MINOR_VER);
    if (_cfgInfo->_subType == SubReqMsg::ST_PART) {
        // ST_PORT:订阅时需要指定集群，ST_ALL:第一次订阅时不需要指定
        std::set<std::string>::iterator it = _cfgInfo->_subClusterSet.begin();
        for (; it != _cfgInfo->_subClusterSet.end(); ++it) {
            ClusterVersion* c_version = _reqMsg->add_cluster_version_vec();
            c_version->set_cluster_name(it->c_str());
            c_version->set_cluster_version(0);
            _mapClusterName2Version[it->c_str()] = 0;
        }
    } else if (_cfgInfo->_subType == SubReqMsg::ST_BELONG) {
        std::set<std::string>::iterator it = _cfgInfo->_subBelongSet.begin();
        for (; it != _cfgInfo->_subBelongSet.end(); ++it) {
            _reqMsg->add_belong_vec(*it);
        }
    }
    return 0;
}

int32_t CMSubscriberImp::subscriber()
{
    // 订阅集群
    int ret = connect();
    if (ret != 0) {
        AUTIL_LOG(ERROR, "cm_sub : CMSubscriberImp::subscriber(), connect() failed !!");
        if (!(_cfgInfo->_readCache && readCache() == 0)) // readCache but failed
        {
            return -1;
        }
    }

    // 创建更新消息线程，但是没有开启
    _threadPtr = autil::Thread::createThread(std::bind(&threadProc, this), "cmSub");
    if (!_threadPtr.get()) {
        AUTIL_LOG(ERROR, "cm_sub : CMSubscriberImp::subscriber(), createThread failed !!");
        return -1;
    }

    if (_cfgInfo->_writeCache) {
        _writeCacheThread =
            autil::Thread::createThread(std::bind(&CMSubscriberImp::writeCacheProc, this), "cmSubCache");

        if (!_writeCacheThread.get()) {
            AUTIL_LOG(ERROR, "cm_sub : create write cache thread failed");
        }
    }
    return 0;
}

/* -----------------  CMSubscriberImp  ------------------ */
int32_t CMSubscriberImp::procSubRespMsg(SubRespMsg* resp_msg)
{
    // 更新所有集群的信息
    if (updateAllClusterInfo(resp_msg) != 0)
        return -1;

    updateReqClusterVersion();
    return 0;
}

void CMSubscriberImp::updateReqClusterVersion()
{
    // 更新所有集群的版本号
    _reqMsg->clear_cluster_version_vec();

    MapClusterName2Version::iterator it = _mapClusterName2Version.begin();
    for (; it != _mapClusterName2Version.end(); ++it) {
        // 如果当前订阅的是所有集群，并且该集群已经被server删除，则不再请求该集群
        if (_cfgInfo->_subType != SubReqMsg::ST_PART && it->second < 0)
            continue;
        ClusterVersion* c_version = _reqMsg->add_cluster_version_vec();
        c_version->set_cluster_name(it->first);
        c_version->set_cluster_version(it->second);
    }
}

void CMSubscriberImp::updateUsedClusters()
{
    _reqMsg->clear_used_clusters();
    std::vector<std::string> names;
    getTopoClusterManager()->getQueriedClusters(names);
    for (size_t i = 0; i < names.size(); ++i) {
        _reqMsg->add_used_clusters(names[i]);
    }
}

int32_t CMSubscriberImp::update()
{
    arpc::ANetRPCController cntler; // rpc contral
    cntler.SetExpireTime(_expireTime);

    updateUsedClusters();
    // 发送同步请求 , 指针在函数内部释放
    cm_basic::SubRespMsg resp_msg;
    _stub->ProcSubMsg(&cntler, _reqMsg, &resp_msg, NULL);

    if (cntler.Failed() || resp_msg.status() == SubRespMsg::SRS_FAILED ||
        resp_msg.status() == SubRespMsg::SRS_INITING) {
        AUTIL_LOG(WARN, "ProcSubMsg failed ErrorText = %s, status %d", cntler.ErrorText().c_str(), resp_msg.status());
        if (delayConnect() != 0) {
            AUTIL_LOG(WARN, "cm_sub : CMSubscriberImp::update(), connect() failed !!");
            return -1;
        }
    } else if (resp_msg.status() == SubRespMsg::SRS_NOT_CM_LEADER) {
        // the server is switching (master <-> slave)
        AUTIL_LOG(WARN, "cm_sub : CMSubscriberImp::update(), resp_msg.status() == SRS_NOT_CM_LEADER");
        // to sleep
        return -1;
    } else if (resp_msg.status() == SubRespMsg::SRS_SUCCESS) {
        AUTIL_LOG(DEBUG, "cm_sub : CMSubscriberImp::update(), resp_msg.status() == SRS_SUCCESS");
        if (procSubRespMsg(&resp_msg) != 0)
            return -1;
    }
    return 0;
}

void* CMSubscriberImp::threadProc(void* argv)
{
    CMSubscriberImp* sub_client = (CMSubscriberImp*)argv;

    while (sub_client->_stopFlag == false) {
        if (sub_client->_stub == NULL && sub_client->connect() != 0) // the first time, not get master from zk
        {
            usleep(500 * 1000);
        } else if (sub_client->update() != 0) {
            AUTIL_LOG(WARN, "cm_sub : CMSubscriberImp::threadProc(), sub_client->update() failed !!");
            usleep(500 * 1000);
        }
    }
    return NULL;
}

int32_t CMSubscriberImp::delayConnect()
{
    int max = 10 * 1000; // 1s
    int d = (int)((float)max * (rand() / (RAND_MAX + 1.0)));
    int step = 10;
    AUTIL_LOG(INFO, "delay %dms to connect to zookeeper", d);
    for (int i = 0; i < d && !_stopFlag; i += step) {
        usleep(step * 1000);
    }
    return _stopFlag ? 0 : connect();
}

int32_t CMSubscriberImp::connect()
{
    std::map<std::string, int> visited;
    while (true) {
        if (_stopFlag == true)
            return 0;

        std::string ip;
        uint16_t port = 0;
        if (!_serverResolver->resolve(ip, port)) {
            AUTIL_LOG(WARN, "cm_sub : resolve server addr failed");
            return -1;
        }
        AUTIL_LOG(WARN, "resolved arpc_spec=tcp:%s:%d", ip.c_str(), port);

        char arpc_spec[IP_LEN];
        snprintf(arpc_spec, IP_LEN, "tcp:%s:%d", ip.c_str(), port);
        visited[arpc_spec] += 1;

        // require resolver resolve by round-robin
        bool all_retried = true;
        for (auto it = visited.begin(); it != visited.end(); ++it) {
            if (it->second <= _retryMax) {
                all_retried = false;
                break;
            }
        }
        if (all_retried) {
            AUTIL_LOG(ERROR, "giveup after all servers (%lu) retried %d times", visited.size(), _retryMax);
            return -1;
        }

        RPCChannel* pChannel = _arpc->OpenChannel(arpc_spec);
        if (pChannel == NULL) {
            AUTIL_LOG(WARN, "cm_sub : CMSubscriberImp _arpc->OpenChannel(%s) failed !!", arpc_spec);
            continue;
        }

        // stub own RPCChannel, it will delete channel while destructing itself
        deletePtr(_stub);
        _stub = new cm_basic::SubscriberService_Stub(pChannel, ::google::protobuf::Service::STUB_OWNS_CHANNEL);

        arpc::ANetRPCController cntler;
        cntler.SetExpireTime(_expireTime);

        // 发送同步请求 , 指针在函数内部释放
        cm_basic::SubRespMsg resp_msg;
        _stub->ProcSubMsg(&cntler, _reqMsg, &resp_msg, NULL);

        if (cntler.Failed() || resp_msg.status() == SubRespMsg::SRS_FAILED) {
            AUTIL_LOG(WARN, "ProcSubMsg failed, status(%d), ErrorText = %s, arpc=tcp:%s:%d", resp_msg.status(),
                      cntler.ErrorText().c_str(), ip.c_str(), port);
            continue;
        }
        // the server is switching (master <-> slave)
        else if (resp_msg.status() == SubRespMsg::SRS_NOT_CM_LEADER) {
            AUTIL_LOG(WARN, "cm_sub : CMSubscriberImp resp_msg.status() == SRS_NOT_CM_LEADER");
            continue;
        } else if (resp_msg.status() == SubRespMsg::SRS_SUCCESS) {
            AUTIL_LOG(DEBUG, "cm_sub : CMSubscriberImp resp_msg.status() == SRS_SUCCESS");
            if (procSubRespMsg(&resp_msg) != 0)
                continue;
            return 0;
        }
        // try another server
        else if (resp_msg.status() == SubRespMsg::SRS_INITING) {
            AUTIL_LOG(INFO, "server %s response SRS_INITING", arpc_spec);
            // tail continue
            continue;
        }
    }

    return -1;
}

int32_t CMSubscriberImp::unsubscriber()
{
    // 停止 更新线程
    _stopFlag = true;

    // 等待 thread_update 线程执行完成
    if (_threadPtr.get()) {
        _threadPtr->join();
        _threadPtr.reset();
    }
    if (_writeCacheThread.get()) {
        _writeCacheThread->join();
        _writeCacheThread.reset();
    }
    return 0;
}

void CMSubscriberImp::writeCacheProc()
{
    int64_t last_write = autil::TimeUtility::currentTime();
    int64_t interval = _cfgInfo->_writeInterval * 1000 * 1000;
    while (!_stopFlag) {
        int64_t now = autil::TimeUtility::currentTime();
        if (now >= last_write + interval) {
            last_write = now;
            doWriteCache();
        }
        usleep(100 * 1000); // 100 ms
    }
}

void CMSubscriberImp::doWriteCache()
{
    AUTIL_LOG(DEBUG, "cm_sub : start to write cluster cache");
    auto clusters = getCMCentral()->getAllCluster();
    std::string content;
    for (size_t i = 0; i < clusters.size(); ++i) {
        auto pCluster = clusters[i]->getCMCluster();
        char* str = pCluster->node_num() == 0 ? NULL : CMClusterXMLBuild::buildXmlCMCluster(pCluster, true);
        if (str == NULL) {
            continue;
        }
        content += str;
        free(str);
    }
    if (cm_basic::FileUtil::fileExist(_cfgInfo->_cacheFile)) {
        // overwrite if target file exists
        cm_basic::FileUtil::renameFile(_cfgInfo->_cacheFile, _cfgInfo->_cacheFile + std::string(".bak"));
    }
    if (!cm_basic::FileUtil::writeFile(_cfgInfo->_cacheFile, std::string(CLUSTERMAP_XML_HEADER) + content +
                                                                 std::string(CLUSTERMAP_XML_HEADER_END))) {
        AUTIL_LOG(WARN, "write cache file <%s> failed", _cfgInfo->_cacheFile);
    }
}

int32_t CMSubscriberImp::readCache()
{
    AUTIL_LOG(INFO, "cm_sub : try load cluster cache from <%s>", _cfgInfo->_cacheFile);
    cm_basic::ClustermapConfig config;
    if (config.parseFile(_cfgInfo->_cacheFile, -1, true) < 0 &&
        config.parseFile((_cfgInfo->_cacheFile + std::string(".bak")).c_str(), -1, true) < 0) {
        AUTIL_LOG(ERROR, "cm_sub : load cache file failed");
        return -1;
    }
    int32_t ret = reinitLocal(config.getAllCluster());
    if (ret == 0) {
        // update subscribe request version to cluster version
        updateReqClusterVersion();
    }
    AUTIL_LOG(INFO, "cm_sub : reinit cluster from cache %s", ret == 0 ? "success" : "failed");
    return ret;
}

int32_t CMSubscriberImp::addSubCluster(std::string name)
{
    autil::ScopedWriteLock lock(_rwlock);
    _reqMsg->clear_cluster_version_vec();
    _mapClusterName2Version[name] = 0;
    if (_cfgInfo->_subType != SubReqMsg::ST_PART && _mapClusterName2Version[name] < 0) {
        return -1;
    }
    ClusterVersion* c_version = _reqMsg->add_cluster_version_vec();
    c_version->set_cluster_name(name);
    c_version->set_cluster_version(0);
    return 0;
}

int32_t CMSubscriberImp::removeSubCluster(std::string name)
{
    autil::ScopedWriteLock lock(_rwlock);
    _reqMsg->clear_cluster_version_vec();
    _mapClusterName2Version.erase(name);
    MapClusterName2Version::iterator it = _mapClusterName2Version.begin();
    for (; it != _mapClusterName2Version.end(); ++it) {
        ClusterVersion* c_version = _reqMsg->add_cluster_version_vec();
        c_version->set_cluster_name(it->first);
        c_version->set_cluster_version(it->second);
    }
    return 0;
}

} // namespace cm_sub
