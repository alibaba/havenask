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
#include "autil/Lock.h"
#include "build_service/admin/ServiceKeeper.h"
#include "catalog/proto/CatalogEntity.pb.h"

namespace catalog::proto {
class ListBuildResponse;
}

namespace build_service { namespace admin {

class CatalogServiceKeeper : public ServiceKeeper
{
public:
    CatalogServiceKeeper(const std::string& catalogAddress, const std::string& catalogName);
    virtual ~CatalogServiceKeeper();

public:
    void stop() override;

public:
    static proto::BuildId transferBuildId(const catalog::proto::BuildId& catalogBuildId);

private:
    bool doInit() override;
    GenerationKeeperPtr doCreateGenerationKeeper(const GenerationKeeper::Param& param) override;

private:
    void catalogLoop();
    bool createCatalogLoop();
    void processBuild(const catalog::proto::BuildId& catalogBuildId, const catalog::proto::BuildTarget& target);
    void startBuild(const catalog::proto::BuildId& catalogBuildId, const std::string& configPath);
    std::string stopBuild(const proto::BuildId& buildId);
    void stopBuild(const catalog::proto::BuildId& catalogBuildId);
    void updateConfig(const catalog::proto::BuildId& catalogBuildId, const std::string& configPath);

private:
    // virtual for test
    virtual bool listBuild(catalog::proto::ListBuildResponse* response);
    virtual void updateBuildCurrent(const catalog::proto::BuildId& catalogBuildId,
                                    const catalog::proto::Build::BuildCurrent& current);

private:
    autil::LoopThreadPtr _catalogThread;
    std::string _catalogAddress;
    std::string _catalogName;
    std::shared_ptr<common::RpcChannelManager> _catalogRpcChannelManager;
    std::map<catalog::proto::BuildId, catalog::proto::BuildTarget> _lastBuildTargets;

    autil::ReadWriteLock _lock;
    std::map<proto::BuildId, catalog::proto::BuildId> _buildIdMap AUTIL_GUARDED_BY(_lock);

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::admin
