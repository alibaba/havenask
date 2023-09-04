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

#include <type_traits>

#include "catalog/entity/EntityId.h"
#include "catalog/proto/CatalogEntityRecord.pb.h"

namespace catalog {

namespace proto {
using TableStructureRecord = ::catalog::proto::TableStructure;
using FunctionRecord = ::catalog::proto::Function;
using LoadStrategyRecord = ::catalog::proto::LoadStrategy;
using UserRecord = ::catalog::proto::User;
using PrivilegeRecord = ::catalog::proto::Privilege;
using OperationLogRecord = ::catalog::proto::OperationLog;
using RootRecord = ::catalog::proto::Root;
} // namespace proto

using CatalogVersion = int64_t;
static constexpr CatalogVersion kInitCatalogVersion = 1;
static constexpr CatalogVersion kInvalidCatalogVersion = -1;
static constexpr CatalogVersion kToUpdateCatalogVersion = kInvalidCatalogVersion;

} // namespace catalog
