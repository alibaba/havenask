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
#include "build_service/admin/catalog/CatalogServiceKeeper.h"

#include <algorithm>
#include <arpc/ANetRPCController.h>
#include <cstddef>
#include <functional>
#include <google/protobuf/service.h>
#include <type_traits>
#include <utility>

#include "alog/Logger.h"
#include "build_service/admin/catalog/CatalogPartitionIdentifier.h"
#include "build_service/admin/catalog/ProtoOperator.h"
#include "build_service/common/RpcChannelManager.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/ProtoComparator.h"
#include "catalog/proto/CatalogService.pb.h"
#include "hippo/DriverCommon.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, CatalogServiceKeeper);

CatalogServiceKeeper::CatalogServiceKeeper(const string& catalogAddress, const string& catalogName)
    : _catalogAddress(catalogAddress)
    , _catalogName(catalogName)
{
}

CatalogServiceKeeper::~CatalogServiceKeeper() {}

void CatalogServiceKeeper::catalogLoop()
{
    catalog::proto::ListBuildResponse response;
    if (!listBuild(&response)) {
        BS_LOG(ERROR, "try list build from catalog [%s] failed", _catalogName.c_str());
        return;
    }

    map<catalog::proto::BuildId, catalog::proto::BuildTarget> buildTargets;
    map<proto::BuildId, catalog::proto::BuildId> buildIdMap;
    for (int i = 0; i < response.build_ids_size(); ++i) {
        const auto& catalogBuildId = response.build_ids(i);
        // invalid build id
        if (catalogBuildId.generation_id() <= 0 || catalogBuildId.partition_name().empty() ||
            catalogBuildId.table_name().empty() || catalogBuildId.database_name().empty() ||
            catalogBuildId.catalog_name().empty()) {
            continue;
        }
        buildTargets.emplace(catalogBuildId, response.build_targets(i));
        const auto& buildId = transferBuildId(catalogBuildId);
        buildIdMap.emplace(buildId, catalogBuildId);
    }
    // target not change
    if (buildTargets == _lastBuildTargets) {
        return;
    }

    // handle remove build
    GenerationKeepers activeGenerationKeepers;
    {
        autil::ScopedLock lock(_mapLock);
        activeGenerationKeepers = _activeGenerationKeepers;
    }
    for (const auto& [buildId, generationKeeper] : activeGenerationKeepers) {
        if (generationKeeper->getGenerationTaskType() == GenerationTaskBase::TT_GENERAL) {
            continue;
        }
        if (buildIdMap.count(buildId) == 0) {
            BS_LOG(INFO, "[%s] is removed from catalog, stop build", buildId.ShortDebugString().c_str());
            stopBuild(buildId);
        }
    }

    // handle add/update build
    {
        autil::ScopedWriteLock lock(_lock);
        _buildIdMap.swap(buildIdMap);
    }
    for (const auto& [catalogBuildId, target] : buildTargets) {
        processBuild(catalogBuildId, target);
    }

    _lastBuildTargets.swap(buildTargets);
}

void CatalogServiceKeeper::processBuild(const catalog::proto::BuildId& catalogBuildId,
                                        const catalog::proto::BuildTarget& target)
{
    catalog::proto::BuildTarget lastTarget;
    if (_lastBuildTargets.count(catalogBuildId)) {
        lastTarget = _lastBuildTargets[catalogBuildId];
    }
    // target not change
    if (target == lastTarget) {
        return;
    }

    BS_LOG(INFO, "target of [%s] changed from [%s] to [%s]", catalogBuildId.ShortDebugString().c_str(),
           lastTarget.ShortDebugString().c_str(), target.ShortDebugString().c_str());
    const auto& buildId = transferBuildId(catalogBuildId);
    bool isRecoverFailed = false;
    auto generationKeeper = getGeneration(buildId, true, isRecoverFailed);
    bool isRunning = generationKeeper != nullptr && !generationKeeper->isStopped() && !generationKeeper->isStopping();

    if (target.type() != catalog::proto::BuildTarget::BATCH_BUILD ||
        target.build_state() != catalog::proto::BuildState::RUNNING) {
        if (isRunning) {
            stopBuild(catalogBuildId);
        }
        return;
    }

    if (target.type() == catalog::proto::BuildTarget::BATCH_BUILD &&
        target.build_state() == catalog::proto::BuildState::RUNNING) {
        if (!isRunning) {
            startBuild(catalogBuildId, target.config_path());
            return;
        }
        if (target.config_path() != generationKeeper->getConfigPath()) {
            updateConfig(catalogBuildId, target.config_path());
            return;
        }
    }
}

void CatalogServiceKeeper::stopBuild(const catalog::proto::BuildId& catalogBuildId)
{
    const auto& buildId = transferBuildId(catalogBuildId);
    auto errorMessage = stopBuild(buildId);
    catalog::proto::Build::BuildCurrent current;
    // success
    if (errorMessage.empty()) {
        current.set_build_state(catalog::proto::BuildState::STOPPED);
    } else {
        current.set_last_error(errorMessage);
    }
    updateBuildCurrent(catalogBuildId, current);
}

string CatalogServiceKeeper::stopBuild(const proto::BuildId& buildId)
{
    proto::StopBuildRequest request;
    *request.mutable_buildid() = buildId;
    proto::InformResponse response;
    ServiceKeeper::stopBuild(&request, &response);
    string errorMessage;
    if (response.errorcode() != proto::ADMIN_ERROR_NONE && response.errormessage_size() > 0) {
        errorMessage = response.errormessage(0);
    }
    return errorMessage;
}

bool CatalogServiceKeeper::listBuild(catalog::proto::ListBuildResponse* response)
{
    arpc::ANetRPCController controller;
    auto rpcChannel = _catalogRpcChannelManager->getRpcChannel();
    if (!rpcChannel) {
        BS_LOG(ERROR, "get rpc channel failed");
        return false;
    }

    catalog::proto::CatalogService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    const int RPC_TIMEOUT = 30 * 1000; // 30s
    controller.SetExpireTime(RPC_TIMEOUT);
    catalog::proto::ListBuildRequest request;
    request.set_catalog_name(_catalogName);
    stub.listBuild(&controller, &request, response, NULL);
    if (controller.Failed()) {
        BS_LOG(ERROR, "rpc listBuild failed, ec:[%d] error:[%s] catalogName[%s]", controller.GetErrorCode(),
               controller.ErrorText().c_str(), _catalogName.c_str());
        return false;
    }
    if (response->status().code() != catalog::proto::ResponseStatus::OK) {
        BS_LOG(ERROR, "rpc listBuild failed, response[%s] catalogName[%s]", response->ShortDebugString().c_str(),
               _catalogName.c_str());
        return false;
    }
    if (response->build_ids_size() != response->build_targets_size()) {
        BS_LOG(ERROR, "size of build_ids [%d] is not equal to size of build_targets [%d], catalogName[%s]",
               response->build_ids_size(), response->build_targets_size(), _catalogName.c_str());
        return false;
    }
    return true;
}

bool CatalogServiceKeeper::createCatalogLoop()
{
    if (_catalogAddress.empty() || _catalogName.empty()) {
        BS_LOG(ERROR, "catalog address [%s] or catalog_name [%s] is empty", _catalogAddress.c_str(),
               _catalogName.c_str());
        return false;
    }
    auto rpcManager = make_shared<common::RpcChannelManager>();
    if (!rpcManager->Init(_catalogAddress, false)) {
        BS_LOG(ERROR, "init rpc failed on [%s]", _catalogAddress.c_str());
        return false;
    }
    _catalogRpcChannelManager = rpcManager;
    _catalogThread =
        autil::LoopThread::createLoopThread(bind(&CatalogServiceKeeper::catalogLoop, this), 5 * 1000 * 1000);
    if (!_catalogThread) {
        BS_LOG(ERROR, "create catalog loop thread failed");
        return false;
    }
    return true;
}

bool CatalogServiceKeeper::doInit()
{
    if (!createCatalogLoop()) {
        BS_LOG(ERROR, "create catalog loop failed");
        return false;
    }

    return true;
}

void CatalogServiceKeeper::stop()
{
    ServiceKeeper::stop();
    _catalogThread.reset();
}

proto::BuildId CatalogServiceKeeper::transferBuildId(const catalog::proto::BuildId& catalogBuildId)
{
    proto::BuildId buildId;
    buildId.set_datatable(catalogBuildId.table_name());
    buildId.set_generationid(catalogBuildId.generation_id());
    buildId.set_appname(catalogBuildId.database_name());
    return buildId;
}

void CatalogServiceKeeper::startBuild(const catalog::proto::BuildId& catalogBuildId, const std::string& configPath)
{
    const auto& buildId = transferBuildId(catalogBuildId);
    proto::StartBuildRequest request;
    request.set_configpath(configPath);
    *request.mutable_buildid() = buildId;
    proto::StartBuildResponse response;
    ServiceKeeper::startBuild(&request, &response);
    string errorMessage;
    if (response.errorcode() != proto::ADMIN_ERROR_NONE && response.errormessage_size() > 0) {
        errorMessage = response.errormessage(0);
    }
    catalog::proto::Build::BuildCurrent current;
    if (errorMessage.empty()) {
        current.set_build_state(catalog::proto::BuildState::RUNNING);
        current.set_config_path(configPath);
    } else {
        current.set_build_state(catalog::proto::BuildState::STOPPED);
        current.set_last_error(errorMessage);
    }
    updateBuildCurrent(catalogBuildId, current);
}
void CatalogServiceKeeper::updateConfig(const catalog::proto::BuildId& catalogBuildId, const std::string& configPath)
{
    const auto& buildId = transferBuildId(catalogBuildId);
    proto::UpdateConfigRequest request;
    *request.mutable_buildid() = buildId;
    request.set_configpath(configPath);
    proto::InformResponse response;
    ServiceKeeper::updateConfig(&request, &response);
    string errorMessage;
    if (response.errorcode() != proto::ADMIN_ERROR_NONE && response.errormessage_size() > 0) {
        errorMessage = response.errormessage(0);
    }
    catalog::proto::Build::BuildCurrent current;
    if (!errorMessage.empty()) {
        current.set_last_error(errorMessage);
    } else {
        current.set_config_path(configPath);
    }
    updateBuildCurrent(catalogBuildId, current);
}
void CatalogServiceKeeper::updateBuildCurrent(const catalog::proto::BuildId& catalogBuildId,
                                              const catalog::proto::Build::BuildCurrent& current)
{
    arpc::ANetRPCController controller;
    auto rpcChannel = _catalogRpcChannelManager->getRpcChannel();
    if (!rpcChannel) {
        BS_LOG(ERROR, "get rpc channel failed");
        return;
    }

    catalog::proto::CatalogService_Stub stub(rpcChannel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);
    const int RPC_TIMEOUT = 30 * 1000; // 30s
    controller.SetExpireTime(RPC_TIMEOUT);
    catalog::proto::UpdateBuildRequest request;
    *request.mutable_build()->mutable_build_id() = catalogBuildId;
    *request.mutable_build()->mutable_current() = current;
    catalog::proto::BuildCommonResponse response;
    stub.updateBuild(&controller, &request, &response, NULL);
    if (controller.Failed()) {
        BS_LOG(ERROR, "[%s] rpc updateBuild failed, ec:[%d] error:[%s]", catalogBuildId.ShortDebugString().c_str(),
               controller.GetErrorCode(), controller.ErrorText().c_str());
        return;
    }
    if (response.status().code() != catalog::proto::ResponseStatus::OK) {
        BS_LOG(ERROR, "[%s] rpc updateBuild failed, response[%s]", catalogBuildId.ShortDebugString().c_str(),
               response.ShortDebugString().c_str());
        return;
    }
}

GenerationKeeperPtr CatalogServiceKeeper::doCreateGenerationKeeper(const GenerationKeeper::Param& param)
{
    catalog::proto::BuildId catalogBuildId;
    {
        autil::ScopedReadLock lock(_lock);
        auto iter = _buildIdMap.find(param.buildId);
        if (iter != _buildIdMap.end()) {
            catalogBuildId = iter->second;
        }
    }
    shared_ptr<CatalogPartitionIdentifier> identifier;
    if (catalogBuildId.generation_id() > 0) {
        identifier =
            make_shared<CatalogPartitionIdentifier>(catalogBuildId.partition_name(), catalogBuildId.table_name(),
                                                    catalogBuildId.database_name(), catalogBuildId.catalog_name());
    }
    return ServiceKeeper::doCreateGenerationKeeper({param.buildId, param.zkRoot, param.adminServiceName,
                                                    param.amonitorPort, param.zkWrapper, param.prohibitIpCollector,
                                                    param.monitor, _catalogRpcChannelManager, identifier});
}

}} // namespace build_service::admin
