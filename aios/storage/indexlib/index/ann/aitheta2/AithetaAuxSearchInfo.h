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

#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/ann/aitheta2/AithetaAuxSearchInfoBase.h"

namespace indexlibv2::index::ann {

class AithetaFilterBase
{
public:
    AithetaFilterBase() = default;
    virtual ~AithetaFilterBase() = default;

public:
    AithetaFilterBase(const AithetaFilterBase&) = delete;
    AithetaFilterBase& operator=(const AithetaFilterBase&) = delete;

public:
    virtual bool operator()(docid_t) const = 0;
    virtual std::string ToString() const = 0;

protected:
    // use _pool to malloc memory instead of using pool from query resource
    autil::mem_pool::Pool _pool;
};

class AithetaAuxSearchInfo : public AithetaAuxSearchInfoBase
{
public:
    AithetaAuxSearchInfo(std::shared_ptr<AithetaFilterBase> filter) : _filter(filter) {}
    ~AithetaAuxSearchInfo() = default;

public:
    std::string GetName() const override { return AithetaAuxSearchInfo::AITHETA_AUXILIARY_SEARCH_INFO; }
    std::string ToString() const override { return _filter->ToString(); }

public:
    const std::shared_ptr<AithetaFilterBase> GetFilter() const { return _filter; }

public:
    static constexpr const char* AITHETA_AUXILIARY_SEARCH_INFO = "aitheta_auxiliary_search_info";

private:
    std::shared_ptr<AithetaFilterBase> _filter;
};

} // namespace indexlibv2::index::ann
