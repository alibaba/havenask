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
#include "aios/network/gig/multi_call/new_heartbeat/BizTopo.h"

#include "aios/network/gig/multi_call/agent/BizStat.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/TopoInfoBuilder.h"
#include "aios/network/gig/multi_call/common/WorkerNodeInfo.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringConvertor.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, BizTopo);

BizTopo::BizTopo(uint64_t publishId, PublishGroupTy group) : _group(group) {
    _signature.publishId = publishId;
}

BizTopo::~BizTopo() {
}

bool BizTopo::init(const ServerBizTopoInfo &info, const std::shared_ptr<BizStat> &bizStat) {
    _bizStat = bizStat;
    _info = info;
    initTopoSignature();
    initMetaSignature();
    initTagSignature();
    return true;
}

void BizTopo::initTopoSignature() {
    int32_t maxLength = _info.bizName.size() + 64 * 4 + 8;
    autil::StringAppender appender(maxLength);
    appender.appendString(_info.bizName)
        .appendChar('_')
        .appendInt64(_info.partCount)
        .appendChar('_')
        .appendInt64(_info.partId)
        .appendChar('_')
        .appendInt64(_info.version)
        .appendChar('_')
        .appendInt64(_info.protocolVersion);
    if (_info.targetWeight < 0) {
        appender.appendChar('_').appendInt64(_info.targetWeight);
    }
    appender.copyToString(_id);
    auto topoSig = autil::HashAlgorithm::hashString64(_id.c_str(), _id.size());
    while (0 == topoSig) {
        _id += "_suffix";
        topoSig = autil::HashAlgorithm::hashString64(_id.c_str(), _id.size());
    }
    _signature.topo = topoSig;
}

void BizTopo::initMetaSignature() {
    int32_t maxLength = 1024;
    autil::StringAppender appender(maxLength);
    for (const auto &pair : _info.metas) {
        appender.appendString(pair.first).appendChar('@').appendString(pair.second).appendChar('$');
    }
    std::string id;
    appender.copyToString(id);
    _signature.meta = autil::HashAlgorithm::hashString64(id.c_str(), id.size());
}

void BizTopo::initTagSignature() {
    int32_t maxLength = 256;
    autil::StringAppender appender(maxLength);
    for (const auto &pair : _info.tags) {
        appender.appendString(pair.first).appendChar('@').appendInt64(pair.second).appendChar('$');
    }
    std::string id;
    appender.copyToString(id);
    _signature.tag = autil::HashAlgorithm::hashString64(id.c_str(), id.size());
}

void BizTopo::fillBizTopoDiff(const BizTopoSignature &clientSideSignature,
                              BizTopoDef *topoDef) const {
    topoDef->set_target_weight(_info.targetWeight);
    fillSignature(topoDef->mutable_signature());
    if (_signature.topo != clientSideSignature.topo) {
        fillTopoKey(topoDef->mutable_key());
    }
    if (_signature.meta != clientSideSignature.meta) {
        fillMeta(topoDef->mutable_metas());
    }
    if (_signature.tag != clientSideSignature.tag) {
        fillTag(topoDef->mutable_tags());
    }
}

void BizTopo::fillBizTopo(BizTopoDef *topoDef) const {
    topoDef->set_target_weight(_info.targetWeight);
    fillSignature(topoDef->mutable_signature());
    fillTopoKey(topoDef->mutable_key());
    fillMeta(topoDef->mutable_metas());
    fillTag(topoDef->mutable_tags());
}

void BizTopo::fillSignature(TopoSignatureDef *signatureDef) const {
    signatureDef->set_topo(_signature.topo);
    signatureDef->set_meta(_signature.meta);
    signatureDef->set_tag(_signature.tag);
    signatureDef->set_publish_id(_signature.publishId);
}

void BizTopo::fillTopoKey(BizTopoKeyDef *keyDef) const {
    keyDef->set_biz_name(_info.bizName);
    keyDef->set_part_count(_info.partCount);
    keyDef->set_part_id(_info.partId);
    keyDef->set_version(_info.version);
    keyDef->set_protocol_version(_info.protocolVersion);
}

void BizTopo::fillMeta(BizMetasDef *metaDef) const {
    for (const auto &pair : _info.metas) {
        const auto &key = pair.first;
        const auto &value = pair.second;
        auto singleMetaDef = metaDef->add_metas();
        singleMetaDef->set_key(key);
        singleMetaDef->set_value(value);
    }
}

void BizTopo::fillTag(BizTagsDef *tagsDef) const {
    autil::ScopedReadWriteLock lock(_tagLock, 'r');
    for (const auto &pair : _info.tags) {
        auto singleTagDef = tagsDef->add_tags();
        singleTagDef->set_tag(pair.first);
        singleTagDef->set_version(pair.second);
    }
}

void BizTopo::fillPropagationStat(BizTopoDef *topoDef) const {
    if (!_bizStat) {
        return;
    }
    auto propStat = topoDef->mutable_propagation_stat();
    _bizStat->fillThisPropagationStat(propStat);
}

void BizTopo::updateVolatileInfo(const BizVolatileInfo &info) {
    _info.targetWeight = info.targetWeight;
    autil::ScopedReadWriteLock lock(_tagLock, 'w');
    _info.tags = info.tags;
    initTagSignature();
}

void BizTopo::stop() {
    _info.targetWeight = MIN_WEIGHT;
}

void BizTopo::addToBuilder(TopoInfoBuilder &builder) const {
    builder.addBiz(_info.bizName, _info.partCount, _info.partId, _info.version, _info.targetWeight,
                   _info.protocolVersion);
}

void BizTopo::toString(std::string &ret) const {
    ret += "  " + _id;
    ret += ", topo_sig: " + autil::StringUtil::toString(_signature.topo);
    ret += ", meta_sig: " + autil::StringUtil::toString(_signature.meta);
    ret += ", tag_sig: " + autil::StringUtil::toString(_signature.tag);
    ret += "\n    target_weight: " + autil::StringUtil::toString(_info.targetWeight);
    ret += "\n    publish_id: " + autil::StringUtil::toString(_signature.publishId);
    ret += "\n    publish_group: " + autil::StringUtil::toString(_group);
    ret += "\n    metas: " + autil::StringUtil::toString(_info.metas);
    {
        autil::ScopedReadWriteLock lock(_tagLock, 'r');
        ret += "\n    tags: " + autil::StringUtil::toString(_info.tags);
    }
    ret += "\n";
}

} // namespace multi_call
