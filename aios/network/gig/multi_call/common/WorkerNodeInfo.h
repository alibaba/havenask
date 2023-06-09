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
#ifndef ISEARCH_MULTI_CALL_WORKERNODEINFO_H
#define ISEARCH_MULTI_CALL_WORKERNODEINFO_H

#include "aios/apps/facility/cm2/cm_basic/basic_struct/proto/cm_common.pb.h"
#include "aios/network/gig/multi_call/common/HbInfo.h"
#include "aios/network/gig/multi_call/common/MetaEnv.h"
#include "aios/network/gig/multi_call/common/Spec.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "autil/StringConvertor.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include <unordered_map>

namespace multi_call {

typedef std::map<std::string, std::string> NodeAttributeMap;

struct TopoNodeMeta : public autil::legacy::Jsonizable {
    TopoNodeMeta();
    ~TopoNodeMeta();

public:
    void init(const std::string &metaStr);
    void setMetaEnv(const MetaEnv &metaEnv);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    bool operator==(const TopoNodeMeta &rhs) const;

private:
    void clear();

public:
    WeightTy targetWeight;
    NodeAttributeMap attributes;
    MetaEnv metaEnv;

private:
    AUTIL_LOG_DECLARE();
};

class ClientTopoInfo;

struct TopoNode {
public:
    TopoNode();

public:
    bool operator==(const TopoNode &rhs) const;
    bool operator<(const TopoNode &rhs) const;
    bool generateNodeId();
    bool validate() const;
    WeightTy getNodeWeight() const;
    void fillNodeMeta(const cm_basic::NodeMetaInfo &metaInfo);
public:
    friend std::ostream& operator<<(std::ostream& os, const TopoNode& node);

public:
    std::string nodeId;
    std::string bizName;
    PartIdTy partCnt;
    PartIdTy partId;
    VersionTy version;
    VersionTy protocalVersion;
    WeightTy weight;
    Spec spec;
    // for log
    std::string clusterName;
    TopoNodeMeta meta;
    // for heartbeat
    HbInfoPtr hbInfo;
    bool supportHeartbeat;
    bool isValid;
    SubscribeType ssType;
    std::shared_ptr<ClientTopoInfo> clientInfo;
};

typedef std::unordered_map<std::string, std::set<TopoNode> > BizInfoMap;
typedef std::vector<TopoNode> TopoNodeVec;
MULTI_CALL_TYPEDEF_PTR(TopoNodeVec);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_WORKERNODEINFO_H
