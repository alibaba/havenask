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
#include "catalog/entity/LoadStrategy.h"

#include <google/protobuf/util/json_util.h>

#include "autil/legacy/md5.h"
#include "catalog/util/ProtoUtil.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, LoadStrategy);

std::string LoadStrategy::getSignature() const {
    proto::LoadStrategy proto;
    auto s = toProto(&proto);
    if (!isOk(s)) {
        return "";
    }
    std::string jsonStr;
    google::protobuf::util::MessageToJsonString(proto, &jsonStr); // warn: map maybe unordered, no map currently
    autil::legacy::Md5Stream stream;
    stream.Put((const uint8_t *)jsonStr.c_str(), jsonStr.length());
    return stream.GetMd5String();
}

void LoadStrategy::copyDetail(LoadStrategy *other) const { other->_loadStrategyConfig = _loadStrategyConfig; }

Status LoadStrategy::fromProto(const proto::LoadStrategy &proto) {
    _status = proto.status();
    _version = proto.version();
    return fromProtoOrCreate(proto);
}

Status LoadStrategy::toProto(proto::LoadStrategy *proto, proto::DetailLevel::Code detailLevel) const {
    proto->mutable_status()->CopyFrom(_status);
    proto->set_version(_version);
    proto->set_table_name(_id.tableName);
    if (detailLevel == proto::DetailLevel::MINIMAL) {
        return StatusBuilder::ok();
    }
    proto->set_table_group_name(_id.tableGroupName);
    proto->set_database_name(_id.databaseName);
    proto->set_catalog_name(_id.catalogName);
    proto->mutable_load_strategy_config()->CopyFrom(_loadStrategyConfig);
    proto->mutable_operation_meta()->CopyFrom(_operationMeta);
    return StatusBuilder::ok();
}

Status LoadStrategy::compare(const LoadStrategy *other, DiffResult &diffResult) const {
    if (other != nullptr && other->version() == version()) {
        return StatusBuilder::ok();
    }
    auto proto = std::make_unique<proto::LoadStrategyRecord>();
    CATALOG_CHECK_OK(toProto(proto.get()));
    diffResult.loadStrategies.emplace_back(std::move(proto));
    return StatusBuilder::ok();
}

void LoadStrategy::updateChildStatus(const proto::EntityStatus &target, UpdateResult &result) {}

void LoadStrategy::alignVersion(CatalogVersion version) {
    if (_version == kToUpdateCatalogVersion) {
        _version = version;
    }
}

Status LoadStrategy::create(const proto::LoadStrategy &request) {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return fromProtoOrCreate(request);
}

Status LoadStrategy::update(const proto::LoadStrategy &request) {
    CATALOG_CHECK(!ProtoUtil::compareProto(_loadStrategyConfig, request.load_strategy_config()),
                  Status::INVALID_ARGUMENTS,
                  "load_strategy_config has no actual diff for table:[",
                  _id.tableName,
                  "] in table_group:[",
                  _id.databaseName,
                  ".",
                  _id.tableGroupName,
                  "]");
    _loadStrategyConfig = request.load_strategy_config();
    _operationMeta = request.operation_meta();
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status LoadStrategy::drop() {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_DELETE);
    return StatusBuilder::ok();
}

Status LoadStrategy::fromProtoOrCreate(const proto::LoadStrategy &proto) {
    _id.tableName = proto.table_name();
    CATALOG_CHECK(!_id.tableName.empty(), Status::INVALID_ARGUMENTS, "table_name is not specified");
    _id.tableGroupName = proto.table_group_name();
    CATALOG_CHECK(!_id.tableGroupName.empty(), Status::INVALID_ARGUMENTS, "table_group_name is not specified");
    _id.databaseName = proto.database_name();
    CATALOG_CHECK(!_id.databaseName.empty(), Status::INVALID_ARGUMENTS, "database_name is not specified");
    _id.catalogName = proto.catalog_name();
    CATALOG_CHECK(!_id.catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    _loadStrategyConfig = proto.load_strategy_config();
    _operationMeta = proto.operation_meta();
    return StatusBuilder::ok();
}

} // namespace catalog
