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

#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolR.h"

namespace suez {
namespace turing {

class QueryMemPoolR : public navi::Resource {
public:
    QueryMemPoolR();
    ~QueryMemPoolR();
    QueryMemPoolR(const QueryMemPoolR &) = delete;
    QueryMemPoolR &operator=(const QueryMemPoolR &) = delete;

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    const autil::mem_pool::PoolPtr &getPool() const;

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    autil::mem_pool::PoolPtr _pool;
};

NAVI_TYPEDEF_PTR(QueryMemPoolR);

} // namespace turing
} // namespace suez
