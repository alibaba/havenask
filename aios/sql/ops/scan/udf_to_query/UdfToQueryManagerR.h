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

#include <string>

#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "sql/ops/scan/udf_to_query/UdfToQueryManager.h"

namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace sql {

class UdfToQueryManagerR : public navi::Resource {
public:
    UdfToQueryManagerR();
    ~UdfToQueryManagerR();
    UdfToQueryManagerR(const UdfToQueryManagerR &) = delete;
    UdfToQueryManagerR &operator=(const UdfToQueryManagerR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    const UdfToQueryManagerPtr &getManager() const;

public:
    static const std::string RESOURCE_ID;

private:
    UdfToQueryManagerPtr _manager;
};

NAVI_TYPEDEF_PTR(UdfToQueryManagerR);

} // namespace sql
