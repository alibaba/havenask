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
#include "navi/engine/NaviHostInfo.h"
#include "navi/proto/GraphVis.pb.h"
#include <fslib/fslib.h>

namespace navi {

void NaviHostInfo::init(const std::string &naviName,
                        const std::string &installRoot,
                        multi_call::GigRpcServer *gigRpcServer)
{
    _naviName = naviName;
    initSpec(gigRpcServer);
    initCommitInfo(installRoot);
}

void NaviHostInfo::fillHostInfo(HostInfoDef *hostInfoDef) const {
    hostInfoDef->set_navi_name(_naviName);
    hostInfoDef->set_git_commit_id(_commitId);
    hostInfoDef->mutable_spec()->CopyFrom(_specDef);
    hostInfoDef->set_packager(_packager);
}

void NaviHostInfo::initSpec(multi_call::GigRpcServer *gigRpcServer) {
    if (!gigRpcServer) {
        return;
    }
    const auto &spec = gigRpcServer->getRpcSpec();
    _specDef.set_ip(spec.ip);
    _specDef.set_tcp_port(spec.ports[multi_call::MC_PROTOCOL_TCP]);
    _specDef.set_arpc_port(spec.ports[multi_call::MC_PROTOCOL_ARPC]);
    _specDef.set_http_port(spec.ports[multi_call::MC_PROTOCOL_HTTP]);
    _specDef.set_grpc_port(spec.ports[multi_call::MC_PROTOCOL_GRPC]);
    _specDef.set_grpc_stream_port(spec.ports[multi_call::MC_PROTOCOL_GRPC_STREAM]);
    _specDef.set_rdma_arpc_port(spec.ports[multi_call::MC_PROTOCOL_RDMA_ARPC]);
}

void NaviHostInfo::initCommitInfo(const std::string &installRoot) {
    auto commitStr = getFileContent(fslib::fs::FileSystem::joinFilePath(installRoot, "usr/local/etc/git_commit"));
    parseCommit(commitStr);
}

void NaviHostInfo::parseCommit(const std::string &valueStr) {
    auto splited = autil::StringUtil::split(valueStr, "\n");
    auto size = splited.size();
    if (size > 0) {
        auto commitSplited = autil::StringUtil::split(splited[0], " ");
        if (2u == commitSplited.size()) {
            _commitId = commitSplited[1];
        }
    }
    if (size > 1) {
        auto userSplited = autil::StringUtil::split(splited[1], " ");
        if (2u == userSplited.size()) {
            _packager = userSplited[1];
        }
    }
}

std::string NaviHostInfo::getFileContent(const std::string &path) {
    if (fslib::EC_TRUE != fslib::fs::FileSystem::isFile(path)) {
        return std::string();
    }
    std::string content;
    if (fslib::EC_OK != fslib::fs::FileSystem::readFile(path, content)) {
        return std::string();
    }
    return content;
}

}

