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
#include "build_service/common/RemoteVersionCommitter.h"

#include <arpc/ANetRPCController.h>
#include <google/protobuf/service.h>
#include <ostream>
#include <stddef.h>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ResourceReader.h"
#include "indexlib/framework/VersionMeta.h"
#include "indexlib/indexlib.h"

namespace {

const size_t RPC_TIMEOUT = autil::EnvUtil::getEnv<size_t>("TABLET_COMMIT_REMOTE_RPC_TIMEOUT", 30 * 1000);
const size_t BACKOFF_WINDOW = autil::EnvUtil::getEnv<size_t>("TABLET_COMMIT_REMOTE_BACKOFF_WINDOW", 10 * 1000);

} // namespace

#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _identifier.c_str(), this, ##args)

namespace build_service::common {
BS_LOG_SETUP(merge, RemoteVersionCommitter);
Status RemoteVersionCommitter::Init(const RemoteVersionCommitter::InitParam& param)
{
    _initParam = param;
    std::stringstream ss;
    ss << _initParam.clusterName << "/"
       << "generation_" << _initParam.generationId << "/partition_" << _initParam.rangeFrom << "_"
       << _initParam.rangeTo;
    _identifier = ss.str();

    auto configPath = param.configPath;
    std::unique_ptr<config::ResourceReader> resourceReader = std::make_unique<config::ResourceReader>(configPath);
    resourceReader->init();

    config::BuildServiceConfig buildServiceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        TABLET_LOG(ERROR, "fail to get build service config");
        return Status::Corruption();
    }

    auto serviceAddress = buildServiceConfig.zkRoot;
    _rpcChannelManager = CreateRpcChannelManager(serviceAddress);
    if (!_rpcChannelManager) {
        TABLET_LOG(ERROR, "create rpc channel manager failed for [%s]", serviceAddress.c_str());
        return Status::Corruption();
    }

    TABLET_LOG(INFO,
               "init RemoteVersionCommitter success, generation [%d], appName [%s], table [%s], dataTable [%s], "
               "range[%d, %d], "
               "configPath[%s], log identifier [%s]",
               _initParam.generationId, _initParam.appName.c_str(), _initParam.clusterName.c_str(),
               _initParam.dataTable.c_str(), (int32_t)_initParam.rangeFrom, (int32_t)_initParam.rangeTo,
               _initParam.configPath.c_str(), _identifier.c_str());
    return Status::OK();
}

Status RemoteVersionCommitter::CommitVersion(const proto::CommitVersionRequest& request,
                                             proto::InformResponse& response)
{
    arpc::ANetRPCController controller;
    auto rpcChannel = _rpcChannelManager->getRpcChannel();
    if (!rpcChannel) {
        TABLET_LOG(ERROR, "get rpc channel failed");
        return Status::Corruption();
    }

    proto::AdminService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    controller.SetExpireTime(RPC_TIMEOUT);
    stub.commitVersion(&controller, &request, &response, NULL);

    if (controller.Failed()) {
        TABLET_LOG(ERROR, "rpc commitVersion failed, ec:[%d] error:[%s]", controller.GetErrorCode(),
                   controller.ErrorText().c_str());
        return Status::Corruption();
    }
    return Status::OK();
}

Status RemoteVersionCommitter::InnerGetCommittedVersions(const proto::GetCommittedVersionsRequest& request,
                                                         proto::InformResponse& response)
{
    arpc::ANetRPCController controller;
    auto rpcChannel = _rpcChannelManager->getRpcChannel();
    if (!rpcChannel) {
        TABLET_LOG(ERROR, "get rpc channel failed");
        return Status::Corruption();
    }
    proto::AdminService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    controller.SetExpireTime(RPC_TIMEOUT);
    stub.getCommittedVersions(&controller, &request, &response, NULL);

    if (controller.Failed()) {
        TABLET_LOG(ERROR, "rpc commitVersion failed, ec:[%d] error:[%s]", controller.GetErrorCode(),
                   controller.ErrorText().c_str());
        return Status::Corruption();
    }
    return Status::OK();
}

Status RemoteVersionCommitter::GetCommittedVersions(uint32_t count, std::vector<versionid_t>& versions)
{
    proto::GetCommittedVersionsRequest request;
    FillBuildId(request.mutable_buildid());
    request.set_clustername(_initParam.clusterName);
    request.mutable_range()->set_from(_initParam.rangeFrom);
    request.mutable_range()->set_to(_initParam.rangeTo);
    request.set_limit(count);

    proto::InformResponse response;
    RETURN_IF_STATUS_ERROR(InnerGetCommittedVersions(request, response), "connect to admin failed");
    if (response.has_errorcode() && response.errorcode() != proto::ADMIN_ERROR_NONE) {
        TABLET_LOG(ERROR, "get committed versions failed, error code [%d]", response.errorcode());
        return Status::Corruption();
    }
    std::string versionsStr = response.response();
    try {
        autil::legacy::FromJsonString(versions, versionsStr);
    } catch (...) {
        TABLET_LOG(ERROR, "convert versions [%s] failed", versionsStr.c_str());
        return Status::Corruption();
    }
    return Status::OK();
}

Status RemoteVersionCommitter::Commit(const indexlibv2::framework::VersionMeta& versionMeta)
{
    proto::CommitVersionRequest request;
    FillBuildId(request.mutable_buildid());
    request.set_clustername(_initParam.clusterName);
    request.mutable_range()->set_from(_initParam.rangeFrom);
    request.mutable_range()->set_to(_initParam.rangeTo);
    request.set_versionmeta(autil::legacy::ToJsonString(versionMeta));

    proto::InformResponse response;
    RETURN_IF_STATUS_ERROR(CommitVersion(request, response), "connect to admin failed");
    if (!response.has_errorcode() || response.errorcode() == proto::ADMIN_ERROR_NONE) {
        TABLET_LOG(INFO, "commit version [%s] success", autil::legacy::ToJsonString(versionMeta).c_str());
        return Status::OK();
    }
    TABLET_LOG(ERROR, "response error code [%d], versionMeta[%s]", response.errorcode(),
               autil::legacy::ToJsonString(versionMeta, true).c_str());
    return Status::Corruption();
}

std::unique_ptr<common::RpcChannelManager>
RemoteVersionCommitter::CreateRpcChannelManager(const std::string& serviceAddress)
{
    auto rpcChannelManager = std::make_unique<common::RpcChannelManager>();
    if (!rpcChannelManager->Init(serviceAddress)) {
        TABLET_LOG(ERROR, "init rpc failed on [%s]", serviceAddress.c_str());
        return nullptr;
    }

    return rpcChannelManager;
}

void RemoteVersionCommitter::FillBuildId(proto::BuildId* buildId) const
{
    if (!_initParam.dataTable.empty()) {
        buildId->set_datatable(_initParam.dataTable);
    }
    buildId->set_generationid(_initParam.generationId);
    if (!_initParam.appName.empty()) {
        buildId->set_appname(_initParam.appName);
    }
}

} // namespace build_service::common
