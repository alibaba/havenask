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

#include "autil/result/Result.h"
#include "catalog/service/CatalogControllerManager.h"
#include "suez/admin/AdminTarget.h"
#include "suez/admin/ClusterManager.h"
#include "suez/sdk/JsonNodeRef.h"

namespace suez {

class TargetGenerator {
public:
    TargetGenerator(const std::shared_ptr<catalog::CatalogControllerManager> &catalogControllerManager,
                    const std::shared_ptr<ClusterManager> &clusterManager);
    ~TargetGenerator();

public:
    autil::Result<AdminTarget> genTarget();

private:
    std::shared_ptr<catalog::CatalogControllerManager> _catalogControllerManager;
    std::shared_ptr<ClusterManager> _clusterManager;
    std::string _templatePath;
    AdminTarget _lastTarget;
    std::string _lastBizConfigSignature;
    std::string _lastBizConfigPath;
};

} // namespace suez
