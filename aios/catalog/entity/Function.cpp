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
#include "catalog/entity/Function.h"

#include "catalog/util/ProtoUtil.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, Function);

void Function::copyDetail(Function *other) const { other->_functionConfig = _functionConfig; }

Status Function::fromProto(const proto::Function &proto) {
    _status = proto.status();
    _version = proto.version();
    return fromProtoOrCreate(proto);
}

Status Function::toProto(proto::Function *proto, proto::DetailLevel::Code detailLevel) const {
    proto->mutable_status()->CopyFrom(_status);
    proto->set_version(_version);
    proto->set_function_name(_id.functionName);
    if (detailLevel == proto::DetailLevel::MINIMAL) {
        return StatusBuilder::ok();
    }
    proto->set_database_name(_id.databaseName);
    proto->set_catalog_name(_id.catalogName);
    proto->mutable_function_config()->CopyFrom(_functionConfig);
    proto->mutable_operation_meta()->CopyFrom(_operationMeta);
    return StatusBuilder::ok();
}

Status Function::compare(const Function *other, DiffResult &diffResult) const {
    if (other != nullptr && other->version() == version()) {
        return StatusBuilder::ok();
    }
    auto proto = std::make_unique<proto::FunctionRecord>();
    CATALOG_CHECK_OK(toProto(proto.get()));
    diffResult.functions.emplace_back(std::move(proto));
    return StatusBuilder::ok();
}

void Function::updateChildStatus(const proto::EntityStatus &target, UpdateResult &result) {}

void Function::alignVersion(CatalogVersion version) {
    if (_version == kToUpdateCatalogVersion) {
        _version = version;
    }
}

Status Function::create(const proto::Function &request) {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return fromProtoOrCreate(request);
}

Status Function::update(const proto::Function &request) {
    CATALOG_CHECK(!ProtoUtil::compareProto(_functionConfig, request.function_config()),
                  Status::INVALID_ARGUMENTS,
                  "function_config has no actual diff for function:[",
                  _id.databaseName,
                  ".",
                  _id.functionName,
                  "]");
    _functionConfig = request.function_config();
    _operationMeta = request.operation_meta();
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Function::drop() {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_DELETE);
    return StatusBuilder::ok();
}

Status Function::fromProtoOrCreate(const proto::Function &proto) {
    _id.functionName = proto.function_name();
    CATALOG_CHECK(!_id.functionName.empty(), Status::INVALID_ARGUMENTS, "function_name is not specified");
    _id.databaseName = proto.database_name();
    CATALOG_CHECK(!_id.databaseName.empty(), Status::INVALID_ARGUMENTS, "database_name is not specified");
    _id.catalogName = proto.catalog_name();
    CATALOG_CHECK(!_id.catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    _functionConfig = proto.function_config();
    _operationMeta = proto.operation_meta();
    return StatusBuilder::ok();
}

} // namespace catalog
