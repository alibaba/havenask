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
#include "ha3/sql/resource/IquanResource.h"
#include "iquan/common/catalog/TableModel.h"
#include "navi/engine/Resource.h"

namespace isearch {
namespace sql {

class IquanSchemaResource : public navi::Resource {
public:
    IquanSchemaResource();
    ~IquanSchemaResource() = default;
    IquanSchemaResource(const IquanSchemaResource &) = delete;
    IquanSchemaResource &operator=(const IquanSchemaResource &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static const std::string RESOURCE_ID;

private:
    IquanResource *_iquanResouce = nullptr;
    iquan::TableModels _sqlTableModels;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IquanSchemaResource> IquanSchemaResourcePtr;

} // namespace sql
} // end namespace isearch
