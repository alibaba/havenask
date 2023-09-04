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
#include "navi/engine/Data.h"
#include "navi/engine/Type.h"

namespace navi {
class TypeContext;
} // namespace navi

namespace sql {

class TableType : public navi::Type {
public:
    TableType();
    ~TableType();
    TableType(const TableType &) = delete;
    TableType &operator=(const TableType &) = delete;

public:
    navi::TypeErrorCode serialize(navi::TypeContext &ctx, const navi::DataPtr &data) const override;
    navi::TypeErrorCode deserialize(navi::TypeContext &ctx, navi::DataPtr &data) const override;

public:
    static const std::string TYPE_ID;
};

} // namespace sql
