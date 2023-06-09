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
#include "suez/service/LocalCatalog.h"

#ifndef AIOS_OPEN_SOURCE
#include "catalog/catalog/CatalogServiceImpl.h"
#endif

#include "suez/sdk/RpcServer.h"

namespace suez {

LocalCatalog::LocalCatalog() {
#ifndef AIOS_OPEN_SOURCE
    _catalogService = std::make_unique<catalog::CatalogServiceImpl>("");
#endif
}

LocalCatalog::~LocalCatalog() {}

bool LocalCatalog::init(const SearchInitParam &initParam) {
#ifndef AIOS_OPEN_SOURCE
    if (!_catalogService->start()) {
        return false;
    }
    return initParam.rpcServer->RegisterService(_catalogService.get());
#else
    return true;
#endif
}

UPDATE_RESULT LocalCatalog::update(const UpdateArgs &updateArgs,
                                   UpdateIndications &updateIndications,
                                   SuezErrorCollector &errorCollector) {
    return UR_REACH_TARGET;
}

void LocalCatalog::stopService() {
#ifndef AIOS_OPEN_SOURCE
    _catalogService->stop();
#endif
}

void LocalCatalog::stopWorker() { stopService(); }

} // namespace suez
