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
#pragma once

#include "navi/common.h"
#include <multi_call/rpc/GigRpcServer.h>
#include "navi/proto/GraphVis.pb.h"

namespace navi {

class NaviHostInfo
{
public:
    NaviHostInfo() {
    }
    ~NaviHostInfo() {
    }
    NaviHostInfo(const NaviHostInfo &) = delete;
    NaviHostInfo &operator=(const NaviHostInfo &) = delete;
public:
    void init(const std::string &naviName, const std::string &installRoot, multi_call::GigRpcServer *gigRpcServer);
    void fillHostInfo(HostInfoDef *hostInfoDef) const;
    const std::string getNaviName() const {
        return _naviName;
    }
private:
    void initSpec(multi_call::GigRpcServer *gigRpcServer);
    void initCommitInfo(const std::string &installRoot);
    void parseCommit(const std::string &valueStr);
private:
    static std::string getFileContent(const std::string &path);
private:
    std::string _naviName;
    std::string _installRoot;
    HostSpecDef _specDef;
    std::string _commitId;
    std::string _packager;
};

NAVI_TYPEDEF_PTR(NaviHostInfo);

}

