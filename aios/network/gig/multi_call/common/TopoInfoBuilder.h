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
#ifndef ISEARCH_MULTI_CALL_TOPOINFOBUILDER_H
#define ISEARCH_MULTI_CALL_TOPOINFOBUILDER_H

#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

struct BizTopoInfo {
    std::string bizName;
    PartIdTy partCnt;
    PartIdTy partId;
    VersionTy version;
    WeightTy targetWeight;
    VersionTy protocolVersion;
    int32_t gigRpcPort;
};

class TopoInfoBuilder {
public:
    TopoInfoBuilder();
    explicit TopoInfoBuilder(int32_t grpcPort);
    ~TopoInfoBuilder();

private:
    TopoInfoBuilder(const TopoInfoBuilder &);
    TopoInfoBuilder &operator=(const TopoInfoBuilder &);

public:
    void addBiz(const std::string &bizName, PartIdTy partCnt, PartIdTy partId,
                VersionTy version, WeightTy targetWeight,
                VersionTy protocolVersion, int32_t gigRpcPort);
    void addBiz(const std::string &bizName, PartIdTy partCnt, PartIdTy partId,
                VersionTy version, WeightTy targetWeight,
                VersionTy protocolVersion);
    void flushAllBizVersion(VersionTy version);
    std::string build() const;
    const std::vector<BizTopoInfo> &getBizTopoInfo() const;

private:
    int32_t _grpcPort;
    std::vector<BizTopoInfo> _infos;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(TopoInfoBuilder);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_TOPOINFOBUILDER_H
