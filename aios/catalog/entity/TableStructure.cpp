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
#include "catalog/entity/TableStructure.h"

#include "catalog/util/ProtoUtil.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, TableStructure);

void TableStructure::copyDetail(TableStructure *other) const {
    other->_columns = _columns;
    other->_indexes = _indexes;
    other->_tableStructureConfig = _tableStructureConfig;
    other->_shardInfo = _shardInfo;
    other->_tableType = _tableType;
    other->_comment = _comment;
}

Status TableStructure::fromProto(const proto::TableStructure &proto) {
    _status = proto.status();
    _version = proto.version();
    return fromProtoOrCreate(proto);
}

Status TableStructure::toProto(proto::TableStructure *proto, proto::DetailLevel::Code detailLevel) const {
    proto->mutable_status()->CopyFrom(_status);
    proto->set_version(_version);
    if (detailLevel == proto::DetailLevel::MINIMAL) {
        return StatusBuilder::ok();
    }
    proto->set_table_name(_id.tableName);
    proto->set_database_name(_id.databaseName);
    proto->set_catalog_name(_id.catalogName);
    for (const auto &column : _columns) {
        proto->add_columns()->CopyFrom(column);
    }
    for (const auto &index : _indexes) {
        proto->add_indexes()->CopyFrom(index);
    }
    proto->mutable_table_structure_config()->CopyFrom(_tableStructureConfig);
    proto->mutable_operation_meta()->CopyFrom(_operationMeta);
    return StatusBuilder::ok();
}

Status TableStructure::compare(const TableStructure *other, DiffResult &diffResult) const {
    if (other != nullptr && other->version() == version()) {
        return StatusBuilder::ok();
    }
    auto proto = std::make_unique<proto::TableStructureRecord>();
    CATALOG_CHECK_OK(toProto(proto.get()));
    diffResult.tableStructures.emplace_back(std::move(proto));
    return StatusBuilder::ok();
}

void TableStructure::alignVersion(CatalogVersion version) {
    if (_version == kToUpdateCatalogVersion) {
        _version = version;
    }
}

void TableStructure::updateChildStatus(const proto::EntityStatus &target, UpdateResult &updateResult) {}

Status TableStructure::create(const proto::TableStructure &request) {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return fromProtoOrCreate(request);
}

Status TableStructure::update(const proto::TableStructure &request) {
    auto oldProto = std::make_unique<proto::TableStructureRecord>();
    CATALOG_CHECK_OK(toProto(oldProto.get()));

    _columns = {request.columns().begin(), request.columns().end()};
    _indexes = {request.indexes().begin(), request.indexes().end()};
    _tableStructureConfig = request.table_structure_config();

    auto newProto = std::make_unique<proto::TableStructureRecord>();
    CATALOG_CHECK_OK(toProto(newProto.get()));

    CATALOG_CHECK(!ProtoUtil::compareProto(*oldProto, *newProto),
                  Status::INVALID_ARGUMENTS,
                  "nothing to update for table_structure in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    _operationMeta = request.operation_meta();
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status TableStructure::drop() {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_DELETE);
    return StatusBuilder::ok();
}

Status TableStructure::addColumn(const proto::TableStructure::Column &request) {
    CATALOG_CHECK(!request.name().empty(), Status::INVALID_ARGUMENTS, "name of column is not specified");
    CATALOG_CHECK(getColumn(request.name()) == _columns.end(),
                  Status::DUPLICATE_ENTITY,
                  "column:[",
                  request.name(),
                  "] already exists in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    _columns.emplace_back(request);
    // TODO(chekong.ygm): validate schema
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status TableStructure::updateColumn(const proto::TableStructure::Column &request) {
    CATALOG_CHECK(!request.name().empty(), Status::INVALID_ARGUMENTS, "name of column is not specified");
    auto it = getColumn(request.name());
    CATALOG_CHECK(it != _columns.end(),
                  Status::NOT_FOUND,
                  "column:[",
                  request.name(),
                  "] not exists in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    CATALOG_CHECK(!ProtoUtil::compareProto(*it, request),
                  Status::INVALID_ARGUMENTS,
                  "column:[",
                  request.name(),
                  "] has no actual diff for table_structure in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    *it = request;
    // TODO(chekong.ygm): validate schema
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status TableStructure::dropColumn(const std::string &columnName) {
    CATALOG_CHECK(!columnName.empty(), Status::INVALID_ARGUMENTS, "name of column is not specified");
    auto it = getColumn(columnName);
    CATALOG_CHECK(it != _columns.end(),
                  Status::NOT_FOUND,
                  "column:[",
                  columnName,
                  "] not exists in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    _columns.erase(it);
    // TODO(chekong.ygm): validate schema
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status TableStructure::createIndex(const proto::TableStructure::Index &request) {
    CATALOG_CHECK(!request.name().empty(), Status::INVALID_ARGUMENTS, "name of index is not specified");
    CATALOG_CHECK(getIndex(request.name()) == _indexes.end(),
                  Status::DUPLICATE_ENTITY,
                  "index:[",
                  request.name(),
                  "] already exists in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    _indexes.emplace_back(request);
    // TODO(chekong.ygm): validate schema
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status TableStructure::updateIndex(const proto::TableStructure::Index &request) {
    CATALOG_CHECK(!request.name().empty(), Status::INVALID_ARGUMENTS, "name of index is not specified");
    auto it = getIndex(request.name());
    CATALOG_CHECK(it != _indexes.end(),
                  Status::NOT_FOUND,
                  "index:[",
                  request.name(),
                  "] not exists in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    CATALOG_CHECK(!ProtoUtil::compareProto(*it, request),
                  Status::INVALID_ARGUMENTS,
                  "index:[",
                  request.name(),
                  "] has no actual diff for table_structure in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    *it = request;
    // TODO(chekong.ygm): validate schema
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status TableStructure::dropIndex(const std::string &indexName) {
    CATALOG_CHECK(!indexName.empty(), Status::INVALID_ARGUMENTS, "name of index is not specified");
    auto it = getIndex(indexName);
    CATALOG_CHECK(it != _indexes.end(),
                  Status::NOT_FOUND,
                  "index:[",
                  indexName,
                  "] not exists in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    _indexes.erase(it);
    // TODO(chekong.ygm): validate schema
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

TableStructure::ColumnIter TableStructure::getColumn(const std::string &name) {
    for (auto it = _columns.begin(); it != _columns.end(); ++it) {
        if (name == it->name()) {
            return it;
        }
    }
    return _columns.end();
}

TableStructure::IndexIter TableStructure::getIndex(const std::string &name) {
    for (auto it = _indexes.begin(); it != _indexes.end(); ++it) {
        if (name == it->name()) {
            return it;
        }
    }
    return _indexes.end();
}

Status TableStructure::fromProtoOrCreate(const proto::TableStructure &proto) {
    _id.tableName = proto.table_name();
    CATALOG_CHECK(!_id.tableName.empty(), Status::INVALID_ARGUMENTS, "table_name is not specified");
    _id.databaseName = proto.database_name();
    CATALOG_CHECK(!_id.databaseName.empty(), Status::INVALID_ARGUMENTS, "database_name is not specified");
    _id.catalogName = proto.catalog_name();
    CATALOG_CHECK(!_id.catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    _columns = {proto.columns().begin(), proto.columns().end()};
    _indexes = {proto.indexes().begin(), proto.indexes().end()};
    _tableStructureConfig = proto.table_structure_config();
    _operationMeta = proto.operation_meta();
    _tableType = _tableStructureConfig.table_type();
    _shardInfo = _tableStructureConfig.shard_info();
    return StatusBuilder::ok();
}

} // namespace catalog
