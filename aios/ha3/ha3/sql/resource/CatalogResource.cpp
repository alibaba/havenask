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
#include "ha3/sql/resource/CatalogResource.h"

#include "iquan/common/catalog/IquanCatalogClient.h"
#include "ha3/sql/common/Log.h"

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, CatalogResource);

const std::string CatalogResource::RESOURCE_ID = "CatalogResource";

CatalogResource::CatalogResource() {}

CatalogResource::CatalogResource(const std::shared_ptr<iquan::IquanCatalogClient> &catalogClient)
    : _catalogClient(catalogClient) {}

void CatalogResource::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID);
}

navi::ErrorCode CatalogResource::init(navi::ResourceInitContext &ctx) {
    if (_catalogClient) {
        SQL_LOG(INFO, "catalog client has exist.");
        return navi::EC_NONE;
    }

    // TODO support to config local/arpc client type
    // _catalogClient.reset(new catalog::CatalogLocalClient());
    return navi::EC_NONE;
}

REGISTER_RESOURCE(CatalogResource);

} // end namespace sql
} // end namespace isearch
