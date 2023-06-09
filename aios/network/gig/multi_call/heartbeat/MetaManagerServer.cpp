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
#include "aios/network/gig/multi_call/heartbeat/MetaManagerServer.h"
#include "aios/network/gig/multi_call/heartbeat/MetaUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, MetaManagerServer);

MetaManagerServer::MetaManagerServer() {}

MetaManagerServer::~MetaManagerServer() {}

bool MetaManagerServer::init() {
    if (!_metaEnv.init()) {
        AUTIL_LOG(WARN, "meta env init failed");
    }
    MetaNode *metaNode = _meta.mutable_meta_node();
    metaNode->set_platform(_metaEnv.getPlatform());
    metaNode->set_hippo_cluster(_metaEnv.getHippoCluster());
    metaNode->set_hippo_app(_metaEnv.getHippoApp());
    metaNode->set_c2_role(_metaEnv.getC2Role());
    metaNode->set_c2_group(_metaEnv.getC2Group());

    return true;
}

bool MetaManagerServer::registerBiz(const std::string &bizName,
                                    const BizMeta &bizMeta) {
    autil::ScopedWriteLock lock(_metaLock);
    auto iter = _bizMetaMap.find(bizName);
    BizMeta *newBizMeta = nullptr;
    if (iter == _bizMetaMap.end()) {
        newBizMeta = _meta.add_biz_meta();
        _bizMetaMap.emplace(bizName, newBizMeta);
    } else if (iter->second->version() != bizMeta.version()) {
        newBizMeta = iter->second;
    }

    if (newBizMeta != nullptr) {
        *newBizMeta = bizMeta;
        newBizMeta->set_biz_name(bizName);
        return true;
    } else {
        return false;
    }
}

bool MetaManagerServer::unregisterBiz(const std::string &bizName) {
    autil::ScopedWriteLock lock(_metaLock);
    auto iter = _bizMetaMap.find(bizName);
    if (iter != _bizMetaMap.end()) {
        _bizMetaMap.erase(iter);
        auto &bizMetas = *(_meta.mutable_biz_meta());
        for (auto bizServiceIter = bizMetas.begin();
             bizServiceIter != bizMetas.end(); ++bizServiceIter) {
            if (bizServiceIter->biz_name() == bizName) {
                bizMetas.erase(bizServiceIter);
                break;
            }
        }
        return true;
    } else {
        return false;
    }
}

const BizMeta *MetaManagerServer::getBiz(const std::string &bizName) const {
    return const_cast<MetaManagerServer *>(this)->getBiz(bizName);
}

BizMeta *MetaManagerServer::getBiz(const std::string &bizName) {
    autil::ScopedWriteLock lock(_metaLock);
    auto iter = _bizMetaMap.find(bizName);
    return iter != _bizMetaMap.end() ? iter->second : nullptr;
}

bool MetaManagerServer::setMeta(const std::string &bizName,
                                const std::string &key,
                                const std::string &value) {
    BizMeta *bizMeta = getBiz(bizName);
    if (bizMeta != nullptr) {
        for (auto &metaKV : *(bizMeta->mutable_extend())) {
            if (metaKV.key() == key) {
                metaKV.set_value(value);
                return true;
            }
        }
        MetaKV *metaKV = bizMeta->add_extend();
        metaKV->set_key(key);
        metaKV->set_value(value);
        return true;
    } else {
        return false;
    }
}

const MetaKV *MetaManagerServer::getMeta(const std::string &bizName,
                                         const std::string &key) const {
    const BizMeta *bizMeta = getBiz(bizName);
    if (bizMeta != nullptr) {
        for (auto &metaKV : bizMeta->extend()) {
            if (metaKV.key() == key) {
                return &metaKV;
            }
        }
    }
    return nullptr;
}

bool MetaManagerServer::delMeta(const std::string &bizName,
                                const std::string &key) {
    BizMeta *biz_meta = getBiz(bizName);
    if (biz_meta != nullptr) {
        auto &metaKVs = *(biz_meta->mutable_extend());
        for (auto iter = metaKVs.begin(); iter != metaKVs.end(); ++iter) {
            auto &metaKV = *iter;
            if (metaKV.key() == key) {
                metaKVs.erase(iter);
                return true;
            }
        }
    }
    return false;
}

/**
 * request : contains only biz_meta info from last request, MetaKV is empty
 * fill in meta if biz_service not in request or version not match
 * suppose MetaKV will not update or create under same (biz, version)
 *combination
 **/
void MetaManagerServer::fillMeta(const HeartbeatRequest &request, Meta *meta) {
    assert(meta != nullptr);
    *(meta->mutable_meta_node()) = _meta.meta_node();

    {
        autil::ScopedWriteLock lock(_metaLock);
        for (auto &pair : _bizMetaMap) {
            const std::string &bizName = pair.second->biz_name();
            const int64_t &version = pair.second->version();
            bool isExisted = false;
            for (auto &client_service_meta : request.biz_meta()) {
                if (client_service_meta.biz_name() == bizName &&
                    client_service_meta.version() == version) {
                    isExisted = true;
                    break;
                }
            }
            if (!isExisted) {
                BizMeta *newBizMeta = meta->add_biz_meta();
                newBizMeta->set_biz_name(bizName);
                newBizMeta->set_version(version);
                MetaUtil::addExtendTo(*(pair.second), newBizMeta, true);
            }
        }
    }
}

} // namespace multi_call
