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

#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Type.h"

namespace isearch {
namespace sql {

class SqlGraphType : public navi::Type
{
public:
    SqlGraphType();
    ~SqlGraphType() = default;
private:
    SqlGraphType(const SqlGraphType &) = delete;
    SqlGraphType& operator=(const SqlGraphType &) = delete;
public:
    navi::TypeErrorCode serialize(navi::TypeContext &ctx, const navi::DataPtr &data) const override;
    navi::TypeErrorCode deserialize(navi::TypeContext &ctx, navi::DataPtr &data) const override;
public:
    static const std::string TYPE_ID;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlGraphType> SqlGraphTypePtr;

} //end namespace data
} //end namespace isearch
