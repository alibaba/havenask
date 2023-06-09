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

#include "autil/Log.h" // IWYU pragma: keep
#include "navi/engine/Resource.h"

namespace iquan {
class IquanCatalogClient;
}

namespace isearch {
namespace sql {

class CatalogResource : public navi::Resource {
public:
    CatalogResource();
    CatalogResource(const std::shared_ptr<iquan::IquanCatalogClient> &catalogClient);
    ~CatalogResource() = default;

    CatalogResource(const CatalogResource &) = delete;
    CatalogResource &operator=(const CatalogResource &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

private:
    std::shared_ptr<iquan::IquanCatalogClient> _catalogClient;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CatalogResource> CatalogResourcePtr;

} // end namespace sql
} // end namespace isearch
