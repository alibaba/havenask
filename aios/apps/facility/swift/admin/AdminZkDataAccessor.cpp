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
#include "swift/admin/AdminZkDataAccessor.h"

#include <algorithm>
#include <assert.h>
#include <limits>
#include <memory>
#include <sstream>
#include <stddef.h>
#include <utility>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/fs/FileSystem.h"
#include "swift/common/PathDefine.h"
#include "swift/protocol/AdminRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h"
#include "swift/util/LogicTopicHelper.h"
#include "swift/util/TargetRecorder.h"
#include "zookeeper/zookeeper.h"

using namespace swift::protocol;
using namespace swift::common;
using namespace swift::util;

using namespace fslib::util;
using namespace http_arpc;
using namespace std;
using namespace autil;

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, AdminZkDataAccessor);

static const int LEADER_HISTORY_MAX_COUNT = 100;

AdminZkDataAccessor::AdminZkDataAccessor() : _recordLocalFile(0) {}

AdminZkDataAccessor::~AdminZkDataAccessor() {}

bool AdminZkDataAccessor::init(cm_basic::ZkWrapper *zkWrapper, const string &sysRoot) {
    _zkPath = PathDefine::getPathFromZkPath(sysRoot);
    return ZkDataAccessor::init(zkWrapper, sysRoot) && mkZkDirs();
}

bool AdminZkDataAccessor::init(const string &sysRoot) {
    _zkPath = PathDefine::getPathFromZkPath(sysRoot);
    return ZkDataAccessor::init(sysRoot);
}

bool AdminZkDataAccessor::mkZkDirs() {
    const string &adminDir = PathDefine::getAdminDir(_zkPath);
    if (!createPath(adminDir)) {
        AUTIL_LOG(ERROR, "create topic dir [%s] failed.", adminDir.c_str());
        return false;
    }
    const string &topicDir = PathDefine::getTopicDir(_zkPath);
    if (!createPath(topicDir)) {
        AUTIL_LOG(ERROR, "create topic dir [%s] failed.", topicDir.c_str());
        return false;
    }
    const string &taskDir = PathDefine::getTaskDir(_zkPath);
    if (!createPath(taskDir)) {
        AUTIL_LOG(ERROR, "create topic dir [%s] failed.", taskDir.c_str());
        return false;
    }
    const string &heartbeatDir = PathDefine::heartbeatMonitorAddress(_zkPath);
    if (!createPath(heartbeatDir)) {
        AUTIL_LOG(ERROR, "create topic dir [%s] failed.", heartbeatDir.c_str());
        return false;
    }
    const string &schemaDir = PathDefine::getSchemaDir(_zkPath);
    if (!createPath(schemaDir)) {
        AUTIL_LOG(ERROR, "create topic dir [%s] failed.", taskDir.c_str());
        return false;
    }
    const string &noUseTopicDir = PathDefine::getNoUseTopicDir(_zkPath);
    if (!createPath(noUseTopicDir)) {
        AUTIL_LOG(ERROR, "create not use topic dir [%s] failed.", taskDir.c_str());
        return false;
    }
    return true;
}

TopicPartitionInfos AdminZkDataAccessor::getTopicPartitionInfos() const { return _topicPartitionInfos; }

bool AdminZkDataAccessor::loadTopicInfos(TopicMetas &topicMetas, TopicPartitionInfos &topicPartInfos) {
    const string &path = PathDefine::getTopicMetaFile(_zkPath);
    bool exist = false;
    if (!_zkWrapper->check(path, exist)) {
        AUTIL_LOG(ERROR, "failed to check whether file exists.");
        return false;
    }
    if (exist) {
        return loadTopicInfosFromNewFiles(topicMetas, topicPartInfos);
    } else {
        return loadTopicInfosFromOldFiles(topicMetas, topicPartInfos);
    }
}

bool AdminZkDataAccessor::loadTopicInfosFromNewFiles(TopicMetas &topicMetas, TopicPartitionInfos &topicPartInfos) {
    if (!readTopicMetas(topicMetas)) {
        AUTIL_LOG(ERROR, "read topic metas from zk failed.");
        return false;
    }
    if (!readTopicPartitionInfos(topicPartInfos)) {
        AUTIL_LOG(WARN, "read partition infos from zk failed, need recover.");
    } else {
        _topicPartitionInfos = topicPartInfos;
    }
    _topicMetas = topicMetas;
    return true;
}

bool AdminZkDataAccessor::recoverPartitionInfos(const TopicMetas &topicMetas, TopicPartitionInfos &partitionInfos) {
    partitionInfos.clear_topicpartitioninfos();
    for (int32_t i = 0; i < topicMetas.topicmetas_size(); i++) {
        const TopicCreationRequest &topicMeta = topicMetas.topicmetas(i);
        TopicPartitionInfo *topicPartitionInfo = partitionInfos.add_topicpartitioninfos();
        topicPartitionInfo->set_topicname(topicMeta.topicname());
        for (uint32_t i = 0; i < topicMeta.partitioncount(); ++i) {
            PartitionInfo *pi = topicPartitionInfo->add_partitioninfos();
            pi->set_id(i);
            pi->set_brokeraddress("");
            pi->set_status(PARTITION_STATUS_NONE);
        }
    }
    if (!writeTopicPartitionInfos(partitionInfos)) {
        AUTIL_LOG(ERROR, "write topic partition infos to zk failed.");
        return false;
    }
    _topicPartitionInfos = partitionInfos;
    return true;
}

bool AdminZkDataAccessor::addTopic(const TopicCreationRequest &topicMeta) {
    TopicBatchCreationRequest metas;
    *metas.add_topicrequests() = topicMeta;
    return addTopics(metas);
}

void AdminZkDataAccessor::addTopicToMetas(const TopicCreationRequest &request,
                                          TopicMetas &metas,
                                          TopicPartitionInfos &partInfos) {
    TopicPartitionInfo topicPartitionInfo;
    topicPartitionInfo.set_topicname(request.topicname());
    PartitionStatus status =
        (TOPIC_TYPE_LOGIC == request.topictype()) ? PARTITION_STATUS_RUNNING : PARTITION_STATUS_NONE;
    for (uint32_t i = 0; i < request.partitioncount(); ++i) {
        PartitionInfo *pi = topicPartitionInfo.add_partitioninfos();
        pi->set_id(i);
        pi->set_brokeraddress("");
        pi->set_status(status);
    }
    *metas.add_topicmetas() = request;
    *partInfos.add_topicpartitioninfos() = topicPartitionInfo;
}

bool AdminZkDataAccessor::addTopics(const TopicBatchCreationRequest &batchTopicMetas) {
    TopicMetas topicMetas = _topicMetas;
    TopicPartitionInfos topicPartitionInfos = _topicPartitionInfos;
    string logTopicNames;
    for (int idx = 0; idx < batchTopicMetas.topicrequests_size(); ++idx) {
        auto &topicMeta = batchTopicMetas.topicrequests(idx);
        logTopicNames += topicMeta.topicname() + ",";
        addTopicToMetas(topicMeta, topicMetas, topicPartitionInfos);
    }
    if (!writeTopicMetas(topicMetas)) {
        AUTIL_LOG(ERROR, "write topic metas to zk failed.");
        return false;
    }
    if (!writeTopicPartitionInfos(topicPartitionInfos)) {
        AUTIL_LOG(ERROR, "write topic partition infos to zk failed.");
    }

    _topicMetas = topicMetas;
    _topicPartitionInfos = topicPartitionInfos;
    AUTIL_LOG(INFO, "add topic name [%s]", logTopicNames.c_str());
    return true;
}

//只有L和PL的topic会需要调用此接口
bool AdminZkDataAccessor::addPhysicTopic(const TopicCreationRequest &request,
                                         int64_t brokerCfgTTlSec,
                                         TopicCreationRequest &retLogicTopicMeta,
                                         TopicCreationRequest &retLastPhysicTopicMeta,
                                         TopicCreationRequest &retNewPhysicTopicMeta) {
    if (TOPIC_TYPE_LOGIC != request.topictype() && TOPIC_TYPE_LOGIC_PHYSIC != request.topictype()) {
        AUTIL_LOG(ERROR,
                  "topic type[%s] cannot add physic topic, request[%s]",
                  TopicType_Name(request.topictype()).c_str(),
                  request.ShortDebugString().c_str());
        return false;
    }
    TopicMetas topicMetas = _topicMetas;
    TopicPartitionInfos topicPartitionInfos = _topicPartitionInfos;
    int64_t curTime = TimeUtility::currentTime();
    // PL topic的part count代表第一个物理topic的属性, 不能修改
    const string &newPhysicTopicName =
        LogicTopicHelper::genPhysicTopicName(request.topicname().c_str(), curTime, request.partitioncount());
    // find logic and modify logic topic
    bool found = false;
    string lastPhyTopicName;
    for (int idx = 0; idx < topicMetas.topicmetas_size(); ++idx) {
        TopicCreationRequest *meta = topicMetas.mutable_topicmetas(idx);
        if (request.topicname() == meta->topicname()) {
            // last physic topic
            int phyLen = meta->physictopiclst_size();
            lastPhyTopicName = (phyLen > 0) ? meta->physictopiclst(phyLen - 1) : meta->topicname();
            // add physic topic
            *meta->add_physictopiclst() = newPhysicTopicName;
            meta->set_modifytime(curTime);
            if (TOPIC_TYPE_LOGIC == meta->topictype()) {
                meta->set_partitioncount(request.partitioncount());
                auto *logicPinfo = topicPartitionInfos.mutable_topicpartitioninfos(idx);
                if (logicPinfo->topicname() == meta->topicname()) {
                    logicPinfo->clear_partitioninfos();
                    for (uint32_t i = 0; i < request.partitioncount(); ++i) {
                        PartitionInfo *pi = logicPinfo->add_partitioninfos();
                        pi->set_id(i);
                        pi->set_status(PARTITION_STATUS_RUNNING);
                    }
                } else {
                    AUTIL_LOG(WARN,
                              "zk id[%d]'s meta[%s] and partinfo[%s] topic dismatch",
                              idx,
                              meta->topicname().c_str(),
                              logicPinfo->topicname().c_str());
                }
            }
            TopicCreationRequest physicMeta = request;
            physicMeta.clear_physictopiclst();
            physicMeta.set_topicname(newPhysicTopicName);
            physicMeta.set_topictype(TOPIC_TYPE_PHYSIC);
            physicMeta.set_createtime(curTime);
            physicMeta.set_modifytime(curTime);
            physicMeta.set_topicexpiredtime(-1);
            physicMeta.set_enablettldel(false);
            physicMeta.set_sealed(false);
            addTopicToMetas(physicMeta, topicMetas, topicPartitionInfos);
            // set return value
            retLogicTopicMeta = *meta;
            retNewPhysicTopicMeta = physicMeta;
            found = true;
            break;
        }
    }
    if (!found) {
        AUTIL_LOG(ERROR, "not found logic topic[%s], add physic topic fail", request.topicname().c_str());
        return false;
    }
    // 上一个物理topic的expire time设上
    for (int idx = 0; idx < topicMetas.topicmetas_size(); ++idx) {
        TopicCreationRequest *meta = topicMetas.mutable_topicmetas(idx);
        if (lastPhyTopicName == meta->topicname()) {
            int64_t topicExpiredTimeSec =
                LogicTopicHelper::getPhysicTopicExpiredTime(curTime, meta->obsoletefiletimeinterval(), brokerCfgTTlSec);
            meta->set_modifytime(curTime);
            meta->set_topicexpiredtime(topicExpiredTimeSec);
            retLastPhysicTopicMeta = *meta;
        }
    }
    if (!writeTopicMetas(topicMetas)) {
        AUTIL_LOG(ERROR, "write topic metas to zk failed");
        return false;
    }
    if (!writeTopicPartitionInfos(topicPartitionInfos)) {
        AUTIL_LOG(ERROR, "write topic partition infos to zk failed.");
    }
    _topicMetas = topicMetas;
    _topicPartitionInfos = topicPartitionInfos;
    AUTIL_LOG(INFO, "add physic topic[%s] success", newPhysicTopicName.c_str());
    return true;
}

bool AdminZkDataAccessor::modifyTopic(const TopicCreationRequest &newMeta) {
    TopicMetas topicMetas = _topicMetas;
    TopicPartitionInfos topicPartitionInfos = _topicPartitionInfos;
    const string &topicName = newMeta.topicname();
    bool found = false;
    for (int i = 0; i < topicMetas.topicmetas_size(); i++) {
        protocol::TopicCreationRequest *topicMetaPtr = topicMetas.mutable_topicmetas(i);
        if (topicMetaPtr->topicname() == topicName) {
            *topicMetaPtr = newMeta;
            found = true;
            break;
        }
    }
    if (!found) {
        AUTIL_LOG(ERROR, "topic[%s] does not exist", topicName.c_str());
        return false;
    }
    bool needUpdatePartitonInfo = false;
    for (int i = 0; i < topicPartitionInfos.topicpartitioninfos_size(); i++) {
        TopicPartitionInfo *partInfo = topicPartitionInfos.mutable_topicpartitioninfos(i);
        if (partInfo->topicname() == topicName &&
            partInfo->partitioninfos_size() != (int32_t)newMeta.partitioncount()) {
            needUpdatePartitonInfo = true;
            partInfo->Clear();
            partInfo->set_topicname(topicName);
            for (uint32_t k = 0; k < newMeta.partitioncount(); ++k) {
                PartitionInfo *pi = partInfo->add_partitioninfos();
                pi->set_id(k);
                pi->set_brokeraddress("");
                pi->set_status(PARTITION_STATUS_NONE);
            }
            break;
        }
    }
    if (!writeTopicMetas(topicMetas)) {
        AUTIL_LOG(ERROR, "write topic metas to zk failed.");
        return false;
    }
    if (needUpdatePartitonInfo && !writeTopicPartitionInfos(topicPartitionInfos)) {
        AUTIL_LOG(ERROR, "write topic partition infos to zk failed.");
    }
    _topicMetas = topicMetas;
    if (needUpdatePartitonInfo) {
        _topicPartitionInfos = topicPartitionInfos;
    }
    AUTIL_LOG(INFO, "modify topic name [%s]", newMeta.topicname().c_str());
    return true;
}

void AdminZkDataAccessor::getUpdateAndDelTopics(const map<string, TopicCreationRequest> &todelTopicMetas,
                                                map<string, TopicCreationRequest> &updateTopicMetas,
                                                set<string> &deleteTopics) {
    for (auto &item : todelTopicMetas) {
        const TopicCreationRequest &meta = item.second;
        if (TOPIC_TYPE_NORMAL == meta.topictype()) {
            deleteTopics.insert(meta.topicname());
        } else if (TOPIC_TYPE_LOGIC == meta.topictype()) {
            // delete logic and all physic topics
            deleteTopics.insert(meta.topicname());
            for (int i = 0; i < meta.physictopiclst_size(); ++i) {
                deleteTopics.insert(meta.physictopiclst(i));
            }
        } else if (TOPIC_TYPE_LOGIC_PHYSIC == meta.topictype()) {
            // only delete first itself physic topic
            if (0 == meta.physictopiclst_size()) {
                AUTIL_LOG(ERROR,
                          "LP topic[%s] delete P will have no physic topic, "
                          "not delete",
                          meta.topicname().c_str());
                continue;
            }
            // update oldest physic topic's enableTTLDel
            TopicCreationRequest physicMeta;
            if (!getTopicMeta(meta.physictopiclst(0), physicMeta)) {
                AUTIL_LOG(ERROR,
                          "LP topic[%s] cannot find physic[%s] meta",
                          meta.ShortDebugString().c_str(),
                          meta.physictopiclst(0).c_str());
                continue;
            }
            TopicCreationRequest lastMeta;
            if (!getTopicMeta(meta.physictopiclst(meta.physictopiclst_size() - 1), lastMeta)) {
                AUTIL_LOG(ERROR, "LP topic[%s] get last physic meta failed", meta.ShortDebugString().c_str());
                continue;
            }
            physicMeta.set_enablettldel(true);
            updateTopicMetas[physicMeta.topicname()] = physicMeta;
            // update logic topic's topic type
            updateTopicMetas[meta.topicname()] = meta;
            TopicCreationRequest &logicMeta = updateTopicMetas[meta.topicname()];
            logicMeta.set_partitioncount(lastMeta.partitioncount());
            logicMeta.set_topictype(TOPIC_TYPE_LOGIC);
            logicMeta.set_topicexpiredtime(-1);
            logicMeta.set_sealed(false);
        } else if (TOPIC_TYPE_PHYSIC == meta.topictype()) {
            // delete physic topic and update its logic topic
            string logicName;
            TopicCreationRequest logicMeta;
            if (!LogicTopicHelper::getLogicTopicName(meta.topicname(), logicName) ||
                !getTopicMeta(logicName, logicMeta)) {
                AUTIL_LOG(WARN, "physic[%s] cannot find logic topic", meta.topicname().c_str());
                continue;
            }
            if (TOPIC_TYPE_LOGIC_PHYSIC == logicMeta.topictype() || logicMeta.physictopiclst(0) != meta.topicname()) {
                AUTIL_LOG(ERROR,
                          "cannot delete middle physic topic[%s], meta[%s]",
                          meta.topicname().c_str(),
                          logicMeta.ShortDebugString().c_str());
                continue;
            }
            if (1 == logicMeta.physictopiclst_size()) {
                AUTIL_LOG(ERROR,
                          "cannot delete one only physic topic[%s] from[%s]",
                          meta.topicname().c_str(),
                          logicMeta.ShortDebugString().c_str());
                continue;
            }
            TopicCreationRequest nextPhysicMeta;
            if (!getTopicMeta(logicMeta.physictopiclst(1), nextPhysicMeta)) {
                AUTIL_LOG(ERROR,
                          "logic[%s]'s physic[%s] cannot get next physic[%s] meta",
                          logicMeta.ShortDebugString().c_str(),
                          meta.topicname().c_str(),
                          logicMeta.physictopiclst(1).c_str());
                continue;
            }
            nextPhysicMeta.set_enablettldel(true);
            updateTopicMetas[nextPhysicMeta.topicname()] = nextPhysicMeta;
            // delete physic topic
            deleteTopics.insert(meta.topicname());
            // update logic topic's physicTopicList
            updateTopicMetas[logicName] = logicMeta;
            TopicCreationRequest &updateMeta = updateTopicMetas[logicName];
            updateMeta.mutable_physictopiclst()->Clear();
            for (int i = 1; i < logicMeta.physictopiclst_size(); ++i) {
                *updateMeta.add_physictopiclst() = logicMeta.physictopiclst(i);
            }
        }
    }
}

bool AdminZkDataAccessor::deleteTopicsAllType(const map<string, TopicCreationRequest> &todelTopicMetas,
                                              set<string> &deletedTopics,
                                              vector<TopicCreationRequest> &updatedTopicMetas) {
    map<string, TopicCreationRequest> toUpdateMetas;
    set<string> toDeleteTopics;
    getUpdateAndDelTopics(todelTopicMetas, toUpdateMetas, toDeleteTopics);
    return deleteTopic(toDeleteTopics, toUpdateMetas, deletedTopics, updatedTopicMetas);
}

bool AdminZkDataAccessor::deleteTopic(const set<string> &topicNames) {
    map<string, TopicCreationRequest> toUpdateMetas;
    set<string> deletedTopics;
    vector<TopicCreationRequest> updatedMetas;
    return deleteTopic(topicNames, toUpdateMetas, deletedTopics, updatedMetas);
}

bool AdminZkDataAccessor::deleteTopic(const set<string> &toDeleteTopics,
                                      const map<string, TopicCreationRequest> &toUpdateMetas,
                                      set<string> &deletedTopics,
                                      vector<TopicCreationRequest> &updatedMetas) {
    TopicMetas topicMetas;
    TopicPartitionInfos topicPartitionInfos;
    assert(_topicMetas.topicmetas_size() == _topicPartitionInfos.topicpartitioninfos_size());
    bool found = false;
    for (int i = 0; i < _topicMetas.topicmetas_size(); i++) {
        const TopicCreationRequest &meta = _topicMetas.topicmetas(i);
        const TopicPartitionInfo &partInfo = _topicPartitionInfos.topicpartitioninfos(i);
        if (toDeleteTopics.count(meta.topicname()) > 0) {
            found = true;
            deletedTopics.insert(meta.topicname());
            continue;
        }
        const auto iter = toUpdateMetas.find(meta.topicname());
        if (toUpdateMetas.end() != iter) {
            found = true;
            *topicMetas.add_topicmetas() = iter->second;
            updatedMetas.emplace_back(iter->second);
            TopicPartitionInfo *newPartInfo = topicPartitionInfos.add_topicpartitioninfos();
            newPartInfo->set_topicname(iter->second.topicname());
            if (TOPIC_TYPE_LOGIC_PHYSIC == meta.topictype() && TOPIC_TYPE_LOGIC == iter->second.topictype()) {
                for (uint32_t i = 0; i < iter->second.partitioncount(); ++i) {
                    PartitionInfo *pi = newPartInfo->add_partitioninfos();
                    pi->set_id(i);
                    pi->set_status(PARTITION_STATUS_RUNNING);
                }
            } else {
                *newPartInfo = partInfo;
            }
        } else {
            *topicMetas.add_topicmetas() = meta;
            *topicPartitionInfos.add_topicpartitioninfos() = partInfo;
        }
    }
    if (!found) {
        AUTIL_LOG(ERROR,
                  "topics [%s] does not exist",
                  StringUtil::toString(toDeleteTopics.begin(), toDeleteTopics.end(), ",").c_str());
        return false;
    }

    if (!writeTopicMetas(topicMetas)) {
        AUTIL_LOG(ERROR, "write topic metas to zk failed.");
        return false;
    }
    if (!writeTopicPartitionInfos(topicPartitionInfos)) {
        AUTIL_LOG(ERROR, "write topic partition infos to zk failed.");
    }
    string deletedTopicsStr;
    for (const auto &topicName : deletedTopics) {
        removeTopicSchema(topicName);
        deletedTopicsStr += topicName + ",";
    }
    _topicMetas = topicMetas;
    _topicPartitionInfos = topicPartitionInfos;
    AUTIL_LOG(INFO, "delete topic name[%s]", deletedTopicsStr.c_str());
    return true;
}

bool AdminZkDataAccessor::setTopicPartitionInfos(const TopicPartitionInfos &topicPartInfos) {
    if (!writeTopicPartitionInfos(topicPartInfos)) {
        AUTIL_LOG(ERROR, "write topic partition infos to zk failed.");
        return false;
    }

    _topicPartitionInfos = topicPartInfos;
    return true;
}

bool AdminZkDataAccessor::writeTopicMetas(const TopicMetas &topicMetas) {
    static int64_t writeCount = 0;
    writeCount++;
    string topicMetaPath = PathDefine::getTopicMetaFile(_zkPath);
    if (_recordLocalFile > 0 && writeCount % _recordLocalFile == 0) {
        string content;
        topicMetas.SerializeToString(&content);
        TargetRecorder::newAdminTopic(content);
    }
    return writeZk(topicMetaPath, topicMetas, true);
}

void AdminZkDataAccessor::setTopicMetas(const TopicMetas &topicMetas) { _topicMetas = topicMetas; }

bool AdminZkDataAccessor::writeTopicPartitionInfos(const TopicPartitionInfos &topicPartitionInfos) {
    static int64_t writeCount = 0;
    writeCount++;
    string topicPartitionInfoPath = PathDefine::getPartitionInfoFile(_zkPath);
    if (_recordLocalFile > 0 && writeCount % _recordLocalFile == 0) {
        string content;
        topicPartitionInfos.SerializeToString(&content);
        TargetRecorder::newAdminPartition(content);
    }
    return writeZk(topicPartitionInfoPath, topicPartitionInfos, true);
}

bool AdminZkDataAccessor::readTopicMetas(TopicMetas &topicMetas) {
    string path = PathDefine::getTopicMetaFile(_zkPath);
    return readZk(path, topicMetas, true);
}

bool AdminZkDataAccessor::readTopicPartitionInfos(TopicPartitionInfos &topicPartitionInfos) {
    string path = PathDefine::getPartitionInfoFile(_zkPath);
    return readZk(path, topicPartitionInfos, true);
}

bool AdminZkDataAccessor::getTopicMeta(const string &topicName, TopicCreationRequest &topicMeta) {
    bool found = false;
    for (int i = 0; i < _topicMetas.topicmetas_size(); i++) {
        const TopicCreationRequest &meta = _topicMetas.topicmetas(i);
        if (meta.topicname() == topicName) {
            found = true;
            topicMeta = meta;
            break;
        }
    }
    if (!found) {
        AUTIL_LOG(ERROR, "topic[%s] does not exist", topicName.c_str());
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::loadTopicInfosFromOldFiles(TopicMetas &topicMetas, TopicPartitionInfos &topicPartInfos) {
    string topicDir = PathDefine::getTopicDir(_zkPath);
    bool exist = false;
    if (!_zkWrapper->check(topicDir, exist)) {
        AUTIL_LOG(ERROR, "fail to check topic dir[%s]", topicDir.c_str());
        return false;
    }
    if (!exist) {
        AUTIL_LOG(INFO, "topic dir[%s] not exist", topicDir.c_str());
        return true;
    }

    vector<string> topics;
    if (!getAllTopicNameOld(topics)) {
        AUTIL_LOG(ERROR, "fail to get topic names");
        return false;
    }
    sort(topics.begin(), topics.end());
    for (size_t i = 0; i < topics.size(); i++) {
        TopicCreationRequest topicMeta;
        TopicPartitionInfo topicPartitionInfo;

        if (!getTopicMetaOld(topics[i], topicMeta)) {
            AUTIL_LOG(ERROR, "fail to get topic meta[%s]", topics[i].c_str());
            return false;
        }

        TopicInfo topicInfo;
        if (!getTopicInfoOld(topics[i], topicInfo)) {
            AUTIL_LOG(ERROR, "fail to get topic info[%s]", topics[i].c_str());
            return false;
        }
        for (int pid = 0; pid < topicInfo.partitioninfos_size(); pid++) {
            *topicPartitionInfo.add_partitioninfos() = topicInfo.partitioninfos(pid);
        }
        topicPartitionInfo.set_topicname(topics[i]);

        *topicMetas.add_topicmetas() = topicMeta;
        *topicPartInfos.add_topicpartitioninfos() = topicPartitionInfo;
    }

    if (topicMetas.topicmetas_size() > 0) {
        if (!writeTopicMetas(topicMetas)) {
            AUTIL_LOG(ERROR, "write topic metas to zk failed.");
            return false;
        }

        if (!writeTopicPartitionInfos(topicPartInfos)) {
            AUTIL_LOG(ERROR, "write topic partition infos to zk failed.");
            return false;
        }
    }

    if (!_zkWrapper->remove(topicDir)) {
        AUTIL_LOG(WARN, "erase old topics dir on zk failed");
    }

    _topicMetas = topicMetas;
    _topicPartitionInfos = topicPartInfos;
    return true;
}

bool AdminZkDataAccessor::getTopicMetaOld(const string &topicName, TopicCreationRequest &meta) const {
    string path = PathDefine::getTopicMetaPath(_zkPath, topicName);
    return readZk(path, meta);
}

bool AdminZkDataAccessor::getTopicInfoOld(const string &topicName, TopicInfo &topicInfo) const {
    string path = PathDefine::getTopicPartitionDir(_zkPath, topicName);
    vector<string> partitions;
    ZOO_ERRORS ec = _zkWrapper->getChild(path, partitions);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "Failed to list dir %s.", path.c_str());
        return false;
    }

    topicInfo.set_name(topicName);
    topicInfo.set_status(TOPIC_STATUS_NONE);

    for (size_t i = 0; i < partitions.size(); i++) {
        PartitionInfo *pi = topicInfo.add_partitioninfos();
        int pid = StringUtil::fromString<uint32_t>(partitions[i]);
        string path = PathDefine::getTopicPartitionPath(_zkPath, topicName, pid);
        readZk(path, *pi);
        pi->set_id(pid);
    }

    return true;
}
bool AdminZkDataAccessor::getAllTopicNameOld(vector<string> &names) const {
    string path = PathDefine::getTopicDir(_zkPath);
    ZOO_ERRORS ec = _zkWrapper->getChild(path, names);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "Failed to list dir %s.", path.c_str());
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::setDispatchedTask(const DispatchInfo &dispatchInfo) {
    return writeZk(PathDefine::getTaskInfoPath(_zkPath, dispatchInfo.rolename()), dispatchInfo);
}

bool AdminZkDataAccessor::getDispatchedTask(const string &roleName, DispatchInfo &dispatchInfo) {
    return readZk(PathDefine::getTaskInfoPath(_zkPath, roleName), dispatchInfo);
}

bool AdminZkDataAccessor::setLeaderInfo(const LeaderInfo &leaderInfo) {
    if (!writeZk(PathDefine::getLeaderInfoFile(_zkPath), leaderInfo)) {
        return false;
    }
    string jsonPath = PathDefine::getLeaderInfoJsonFile(_zkPath);
    if (!writeZk(jsonPath, leaderInfo, false, true)) {
        AUTIL_LOG(ERROR, "write leader json file Failed [%s].", jsonPath.c_str());
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::getLeaderInfo(LeaderInfo &leaderInfo) const {
    return readZk(PathDefine::getLeaderInfoFile(_zkPath), leaderInfo);
}

bool AdminZkDataAccessor::addLeaderInfoToHistory(const LeaderInfo &leaderInfo) {
    string path = PathDefine::getLeaderHistoryFile(_zkPath);
    LeaderInfoHistory leaderInfoHistory;
    LeaderInfo *newLeaderInfo = leaderInfoHistory.add_leaderinfos();
    newLeaderInfo->CopyFrom(leaderInfo);
    bool exist = false;
    if (_zkWrapper->check(path, exist) && exist) {
        LeaderInfoHistory oldLeaderInfoHistory;
        readZk(path, oldLeaderInfoHistory);
        for (int i = 0; i < min(oldLeaderInfoHistory.leaderinfos().size(), LEADER_HISTORY_MAX_COUNT - 1); i++) {
            LeaderInfo *newLeaderInfo = leaderInfoHistory.add_leaderinfos();
            newLeaderInfo->CopyFrom(oldLeaderInfoHistory.leaderinfos(i));
        }
    }
    if (!writeZk(path, leaderInfoHistory)) {
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::getHistoryLeaders(LeaderInfoHistory &historyLeaders) const {
    return readZk(PathDefine::getLeaderHistoryFile(_zkPath), historyLeaders);
}

bool AdminZkDataAccessor::readConfigVersion(std::string &versionStr) {
    string path = PathDefine::getConfigVersionFile(_zkPath);
    ZOO_ERRORS ec = _zkWrapper->getData(path, versionStr);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "fail to get data %s", path.c_str());
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::writeConfigVersion(const std::string &versionStr) {
    string path = PathDefine::getConfigVersionFile(_zkPath);
    if (!writeFile(path, versionStr)) {
        AUTIL_LOG(ERROR, "fail to write data %s", path.c_str());
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::readLeaderAddress(const std::string &roleName, std::string &leaderAddress) {
    string address;
    string path = PathDefine::getLeaderFilePath(_zkPath, roleName);
    ZOO_ERRORS ec = _zkWrapper->getData(path, address);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "read leader address for role [%s] failed, path [%s].", roleName.c_str(), path.c_str());
        return false;
    }
    // support leader file have both rpc and http address
    const vector<string> &addrVec = StringUtil::split(address, "\n");
    if (addrVec.empty()) {
        leaderAddress = address;
    } else {
        leaderAddress = addrVec[0];
    }
    return true;
}

bool AdminZkDataAccessor::writeLeaderAddress(const string &roleName, const string &leaderAddress) {
    string path = PathDefine::getLeaderFilePath(_zkPath, roleName);
    bool isSuc = writeFile(path, leaderAddress);
    if (!isSuc) {
        AUTIL_LOG(ERROR, "write leader address for role [%s] failed, path [%s].", roleName.c_str(), path.c_str());
    }
    return isSuc;
}

bool AdminZkDataAccessor::setBrokerVersionInfos(const RoleVersionInfos &roleVersionInfos) {
    if (_versionInfos == roleVersionInfos) {
        return true;
    }
    const string &path = PathDefine::getBrokerVersionsFile(_zkPath);
    bool isSuc = writeZk(path, roleVersionInfos);
    if (!isSuc) {
        AUTIL_LOG(ERROR, "write broker version failed, path [%s].", path.c_str());
    } else {
        _versionInfos = roleVersionInfos;
    }
    return isSuc;
}

bool AdminZkDataAccessor::getBrokerVersionInfos(RoleVersionInfos &roleVersionInfos) {
    const string &path = PathDefine::getBrokerVersionsFile(_zkPath);
    bool isSuc = readZk(path, roleVersionInfos);
    if (!isSuc) {
        AUTIL_LOG(ERROR, "write broker version failed, path [%s].", path.c_str());
    }
    return isSuc;
}

bool AdminZkDataAccessor::readTopicMetas(string &content) {
    const string &path = PathDefine::getTopicMetaFile(_zkPath);
    ZOO_ERRORS ec = _zkWrapper->getData(path, content);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "fail to get data %s", path.c_str());
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::loadTopicSchemas() {
    bool exist = false;
    const string &path = PathDefine::getSchemaDir(_zkPath);
    if (!_zkWrapper->check(path, exist)) {
        AUTIL_LOG(ERROR, "fail to check schema dir[%s]", path.c_str());
        return false;
    }
    if (!exist) {
        AUTIL_LOG(ERROR, "schema dir[%s] not exist", path.c_str());
        return false;
    }
    vector<string> topics;
    ZOO_ERRORS ec = _zkWrapper->getChild(path, topics);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "Failed to list dir %s.", path.c_str());
        return false;
    }
    map<string, SchemaInfos> topicSchemas;
    for (size_t index = 0; index < topics.size(); index++) {
        SchemaInfos schemaInfos;
        const string &path = PathDefine::getTopicSchemaFile(_zkPath, topics[index]);
        if (!readZk(path, schemaInfos, true)) {
            AUTIL_LOG(ERROR, "fail to get topic schema[%s], continue", topics[index].c_str());
            continue;
        }
        if (0 != schemaInfos.sinfos_size()) {
            topicSchemas.insert({topics[index], schemaInfos});
            AUTIL_LOG(INFO, "load topic[%s] schema success", topics[index].c_str());
        }
    }
    _topicSchemas = topicSchemas;
    return true;
}

bool AdminZkDataAccessor::getOldestSchemaVersion(const SchemaInfos &scmInfos, int32_t &version, int &pos) {
    uint32_t oldestTime = std::numeric_limits<uint32_t>::max();
    for (int64_t index = 0; index < scmInfos.sinfos_size(); ++index) {
        if (oldestTime > scmInfos.sinfos(index).registertime()) {
            oldestTime = scmInfos.sinfos(index).registertime();
            version = scmInfos.sinfos(index).version();
            pos = index;
        }
    }
    return true;
}

bool AdminZkDataAccessor::writeTopicSchema(const std::string &topicName, const SchemaInfos &scmInfos) {
    const string &path = PathDefine::getTopicSchemaFile(_zkPath, topicName);
    bool isSuc = writeZk(path, scmInfos, true);
    if (!isSuc) {
        AUTIL_LOG(ERROR, "write topic[%s] schema failed, path[%s]", topicName.c_str(), path.c_str());
    }
    return isSuc;
}

bool AdminZkDataAccessor::addTopicSchema(const string &topicName,
                                         int32_t version,
                                         const string &schema,
                                         int32_t &removedVersion,
                                         uint32_t maxAllowSchemaNum) {
    SchemaInfo si;
    si.set_version(version);
    si.set_registertime((uint32_t)TimeUtility::currentTimeInSeconds());
    si.set_schema(schema);
    SchemaInfos scmInfos = _topicSchemas[topicName];
    if ((int)maxAllowSchemaNum <= scmInfos.sinfos_size()) {
        int index = 0;
        if (getOldestSchemaVersion(scmInfos, removedVersion, index)) {
            AUTIL_LOG(INFO, "remove obsolete version[%d]", removedVersion);
            *scmInfos.mutable_sinfos(index) = si;
        }
    } else {
        *scmInfos.add_sinfos() = si;
    }
    if (!writeTopicSchema(topicName, scmInfos)) {
        AUTIL_LOG(ERROR, "write topic schema to zk failed.");
        return false;
    }
    _topicSchemas[topicName] = scmInfos;
    AUTIL_LOG(INFO, "add topic[%s] schema[%s] version[%d]", topicName.c_str(), schema.c_str(), version);
    return true;
}

bool AdminZkDataAccessor::getSchema(const string &topicName, int32_t version, SchemaInfo &schemaInfo) {
    const auto iter = _topicSchemas.find(topicName);
    if (_topicSchemas.end() == iter) {
        AUTIL_LOG(WARN, "topic[%s] schema not found", topicName.c_str());
        return false;
    }
    bool retValue = false;
    const SchemaInfos &infos = iter->second;
    if (0 == version) { // need latest
        uint32_t latestTime = 0;
        for (int64_t index = 0; index < infos.sinfos_size(); ++index) {
            if (latestTime < infos.sinfos(index).registertime()) {
                latestTime = infos.sinfos(index).registertime();
                schemaInfo = infos.sinfos(index);
                retValue = true;
            }
        }
    } else {
        for (int64_t index = 0; index < infos.sinfos_size(); ++index) {
            if (version == infos.sinfos(index).version()) {
                schemaInfo = infos.sinfos(index);
                retValue = true;
            }
        }
    }
    return retValue;
}

bool AdminZkDataAccessor::removeTopicSchema(const string &topicName) {
    const auto iter = _topicSchemas.find(topicName);
    if (_topicSchemas.end() == iter) {
        return true;
    }
    const string &path = PathDefine::getTopicSchemaFile(_zkPath, topicName);
    if (remove(path)) {
        _topicSchemas.erase(iter);
        AUTIL_LOG(INFO, "remove topic[%s] schema success", topicName.c_str());
    }
    return true;
}

bool AdminZkDataAccessor::writeTopicRWTime(const TopicRWInfos &rwInfos) {
    const string &rwFile = PathDefine::getTopicRWFile(_zkPath);
    return writeZk(rwFile, rwInfos, true);
}

bool AdminZkDataAccessor::loadTopicRWTime(TopicRWInfos &rwInfos) {
    const string &path = PathDefine::getTopicRWFile(_zkPath);
    return readZk(path, rwInfos, true);
}

bool AdminZkDataAccessor::writeDeletedNoUseTopics(const TopicMetas &metas, uint32_t maxAllowNum) {
    const string &noUseTopicDir = PathDefine::getNoUseTopicDir(_zkPath);
    vector<string> topicFiles;
    ZOO_ERRORS ec = _zkWrapper->getChild(noUseTopicDir, topicFiles);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "Failed to list dir %s.", noUseTopicDir.c_str());
        return false;
    }
    if (topicFiles.size() >= maxAllowNum) {
        size_t deleteCnt = topicFiles.size() - maxAllowNum + 1;
        std::sort(topicFiles.begin(), topicFiles.end()); //增序
        for (size_t i = 0; i < deleteCnt; ++i) {
            const string &oldestFile = fslib::fs::FileSystem::joinFilePath(noUseTopicDir, topicFiles[i]);
            if (_zkWrapper->deleteNode(oldestFile)) {
                AUTIL_LOG(INFO, "delete oldest NoUseTopic file[%s] success", oldestFile.c_str());
            } else {
                AUTIL_LOG(ERROR, "delete oldest NoUseTopic file[%s] fail", oldestFile.c_str());
            }
        }
    }
    const string &newTopicFile =
        fslib::fs::FileSystem::joinFilePath(noUseTopicDir, StringUtil::toString(TimeUtility::currentTimeInSeconds()));
    AUTIL_LOG(INFO, "write deleted NoUseTopic file[%s]", newTopicFile.c_str());
    return writeZk(newTopicFile, metas, true);
}

bool AdminZkDataAccessor::loadLastDeletedNoUseTopics(TopicMetas &metas) {
    string lastFileName;
    if (!getLastDeletedNoUseTopicFileName(lastFileName)) {
        return false;
    }
    if (lastFileName.empty()) {
        return true;
    }
    return loadDeletedNoUseTopics(lastFileName, metas);
}

bool AdminZkDataAccessor::getLastDeletedNoUseTopicFileName(string &fileName) {
    vector<string> topicFiles;
    if (!loadDeletedNoUseTopicFiles(topicFiles)) {
        return false;
    }
    if (0 == topicFiles.size()) {
        AUTIL_LOG(INFO, "deleted NoUseTopic dir empty");
        return true;
    }
    std::sort(topicFiles.begin(), topicFiles.end()); //增序
    fileName = topicFiles[topicFiles.size() - 1];
    return true;
}

bool AdminZkDataAccessor::loadDeletedNoUseTopics(const string &fileName, TopicMetas &metas) {
    const string &noUseTopicDir = PathDefine::getNoUseTopicDir(_zkPath);
    const string &noUseFile = fslib::fs::FileSystem::joinFilePath(noUseTopicDir, fileName);
    return readZk(noUseFile, metas, true);
}

bool AdminZkDataAccessor::loadDeletedNoUseTopicFiles(vector<string> &topicFiles) {
    const string &noUseTopicDir = PathDefine::getNoUseTopicDir(_zkPath);
    ZOO_ERRORS ec = _zkWrapper->getChild(noUseTopicDir, topicFiles);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "Failed to list dir[%s]", noUseTopicDir.c_str());
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::loadChangePartCntTask() {
    const string &path = PathDefine::getChangePartCntTaskFile(_zkPath);
    bool exist = false;
    if (!_zkWrapper->check(path, exist)) {
        AUTIL_LOG(WARN, "failed to check file[%s] exists", path.c_str());
        return false;
    }
    if (!exist) {
        AUTIL_LOG(INFO, "change part count task[%s] not exist", path.c_str());
        return true;
    }
    protocol::ChangeTopicPartCntTasks changePartCntTasks;
    bool retVal = readZk(path, changePartCntTasks, true);
    if (retVal) {
        AUTIL_LOG(
            INFO, "load change partcnt task[%s] success, task size[%d]", path.c_str(), changePartCntTasks.tasks_size());
    } else {
        AUTIL_LOG(ERROR, "load change partcnt task[%s] fail", path.c_str());
        return false;
    }
    ScopedWriteLock lock(_changePartCntTasksLock);
    _changePartCntTasks = changePartCntTasks;
    return retVal;
}

ErrorCode AdminZkDataAccessor::sendChangePartCntTask(const TopicCreationRequest &request) {
    ScopedWriteLock lock(_changePartCntTasksLock);
    ChangeTopicPartCntTasks tasks = _changePartCntTasks;
    for (int idx = tasks.tasks_size() - 1; idx >= 0; --idx) {
        const auto &meta = tasks.tasks(idx).meta();
        if (request.topicname() == meta.topicname()) {
            AUTIL_LOG(
                ERROR, "topic[%s] has task[%s] doing..", request.topicname().c_str(), meta.ShortDebugString().c_str());
            return ERROR_ADMIN_CHG_PART_TASK_NOT_FINISH;
        }
    }
    ChangeTopicPartCntTask *taskPtr = tasks.add_tasks();
    taskPtr->set_taskid(request.modifytime());
    *taskPtr->mutable_meta() = request;
    const string &tasksFilePath = PathDefine::getChangePartCntTaskFile(_zkPath);
    if (!writeZk(tasksFilePath, tasks, true)) {
        AUTIL_LOG(ERROR, "write change partcnt task[%s] to zk failed", request.ShortDebugString().c_str());
        return ERROR_ADMIN_OPERATION_FAILED;
    }
    _changePartCntTasks = tasks;
    AUTIL_LOG(INFO,
              "write change partcnt task[%s] to zk success, task size[%d]",
              request.ShortDebugString().c_str(),
              tasks.tasks_size());
    return ERROR_NONE;
}

void AdminZkDataAccessor::updateChangePartCntTasks(const set<int64_t> &finishedIds) {
    if (0 == finishedIds.size()) {
        return;
    }
    ScopedWriteLock lock(_changePartCntTasksLock);
    ChangeTopicPartCntTasks newTasks;
    for (int idx = _changePartCntTasks.tasks_size() - 1; idx >= 0; --idx) {
        const auto &task = _changePartCntTasks.tasks(idx);
        if (finishedIds.find(task.meta().modifytime()) == finishedIds.end()) {
            *newTasks.add_tasks() = task;
        }
    }
    const string &tasksFilePath = PathDefine::getChangePartCntTaskFile(_zkPath);
    if (!writeZk(tasksFilePath, newTasks, true)) {
        AUTIL_LOG(ERROR, "write change partcnt task[%s] to zk failed", newTasks.ShortDebugString().c_str());
    } else {
        _changePartCntTasks = newTasks;
        AUTIL_LOG(INFO, "update change partcnt tasks success, task size[%d]", newTasks.tasks_size());
    }
}

const ChangeTopicPartCntTasks AdminZkDataAccessor::getChangePartCntTasks() const {
    ScopedReadLock lock(_changePartCntTasksLock);
    return _changePartCntTasks;
}

bool AdminZkDataAccessor::writeMasterVersion(uint64_t targetVersion) {
    const string &path = PathDefine::getSelfMasterVersionFile(_zkPath);
    if (!writeFile(path, StringUtil::toString(targetVersion))) {
        AUTIL_LOG(ERROR, "fail to write data %s", path.c_str());
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::readMasterVersion(uint64_t &masterVersion) {
    const string &path = PathDefine::getSelfMasterVersionFile(_zkPath);
    bool exist = false;
    if (!_zkWrapper->check(path, exist)) {
        AUTIL_LOG(ERROR, "fail to check master version file[%s]", path.c_str());
        return false;
    }
    if (!exist) {
        AUTIL_LOG(INFO, "master version file[%s] not exist", path.c_str());
        return true;
    }
    std::string content;
    auto ec = _zkWrapper->getData(path, content);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "fail to get data %s", path.c_str());
        return false;
    }
    if (!StringUtil::strToUInt64(content.c_str(), masterVersion)) {
        AUTIL_LOG(ERROR, "fail to convert version[%s] to uint64", content.c_str());
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::serializeCleanAtDeleteTasks(const protocol::CleanAtDeleteTopicTasks &tasks) {
    string toDelTopicsPath = PathDefine::getCleanAtDeleteTaskFile(_zkPath);
    if (!writeZk(toDelTopicsPath, tasks, true, false)) {
        AUTIL_LOG(WARN, "write clean at delete topic plan fail");
        return false;
    }
    return true;
}

bool AdminZkDataAccessor::loadCleanAtDeleteTopicTasks(CleanAtDeleteTopicTasks &tasks) {
    const string &path = PathDefine::getCleanAtDeleteTaskFile(_zkPath);
    bool exist = false;
    if (!_zkWrapper->check(path, exist)) {
        AUTIL_LOG(WARN, "failed to check file[%s] exists", path.c_str());
        return false;
    }
    if (!exist) {
        AUTIL_LOG(INFO, "clean at delete task[%s] not exist", path.c_str());
        return true;
    }
    bool retVal = readZk(path, tasks, true, false);
    if (retVal) {
        AUTIL_LOG(INFO, "load clean at delete task[%s] success, task size[%d]", path.c_str(), tasks.tasks_size());
    } else {
        AUTIL_LOG(ERROR, "load clean at delete task[%s] fail", path.c_str());
        return false;
    }
    return retVal;
}

void AdminZkDataAccessor::removeOldWriterVersionData(const std::set<std::string> &topicNames) {
    if (topicNames.empty()) {
        return;
    }
    const std::string &zkPath = PathDefine::getWriteVersionControlPath(_zkPath);
    std::vector<std::string> topics;
    ZOO_ERRORS ec = _zkWrapper->getChild(zkPath, topics);
    if (ZOK != ec) {
        AUTIL_LOG(ERROR, "Failed to list dir[%s]", zkPath.c_str());
        return;
    }
    for (const auto &topic : topics) {
        if (topicNames.end() == topicNames.find(topic)) {
            const std::string &path = fslib::fs::FileSystem::joinFilePath(zkPath, topic);
            if (_zkWrapper->remove(path)) {
                AUTIL_LOG(INFO, "remove topic[%s] writer version success", topic.c_str());
            } else {
                AUTIL_LOG(WARN, "remove topic[%s] writer version fail", topic.c_str());
            }
        }
    }
}

} // namespace admin
} // namespace swift
