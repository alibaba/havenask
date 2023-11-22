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

#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "build_service/common/RpcChannelManager.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {
class VersionMeta;
}

using indexlib::Status;

namespace build_service::common {

class RemoteVersionCommitter
{
public:
    struct InitParam {
        int32_t generationId = -1;
        std::string dataTable;
        std::string appName;
        std::string clusterName;
        uint16_t rangeFrom = 0;
        uint16_t rangeTo = 0;
        std::string configPath;
    };

public:
    RemoteVersionCommitter() = default;
    virtual ~RemoteVersionCommitter() = default;

    virtual Status Init(const InitParam& param);
    virtual Status Commit(const indexlibv2::framework::VersionMeta& versionMeta);
    virtual Status GetCommittedVersions(uint32_t count, std::vector<indexlibv2::versionid_t>& versions);

private:
    void FillBuildId(proto::BuildId* buildId) const;
    std::unique_ptr<common::RpcChannelManager> CreateRpcChannelManager(const std::string& serviceAddress);
    // virtual for test
    virtual Status CommitVersion(const proto::CommitVersionRequest& request, proto::InformResponse& response);
    virtual Status InnerGetCommittedVersions(const proto::GetCommittedVersionsRequest& request,
                                             proto::InformResponse& response);

private:
    InitParam _initParam;
    std::string _identifier;
    std::unique_ptr<common::RpcChannelManager> _rpcChannelManager;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::common
