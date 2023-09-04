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
#include "catalog/entity/Table.h"

#include "catalog/util/ProtoUtil.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, Table);

void Table::copyDetail(Table *other) const {
    other->_tableConfig = _tableConfig;
    other->_tableStructure = _tableStructure->clone();
    for (const auto &it : _partitions) {
        other->_partitions.emplace(it.first, it.second->clone());
    }
}

template <typename T>
inline Status checkSubProto(const TableId &id, const T &subProto) {
    CATALOG_CHECK(id.tableName == subProto.table_name(),
                  Status::INVALID_ARGUMENTS,
                  "table_name:[",
                  subProto.table_name(),
                  "] conflict with table:[",
                  id.databaseName,
                  ".",
                  id.tableName,
                  "] in catalog:[",
                  id.catalogName,
                  "]");
    CATALOG_CHECK(id.databaseName == subProto.database_name(),
                  Status::INVALID_ARGUMENTS,
                  "database_name:[",
                  subProto.database_name(),
                  "] conflict with table:[",
                  id.databaseName,
                  ".",
                  id.tableName,
                  "] in catalog:[",
                  id.catalogName,
                  "]");
    CATALOG_CHECK(id.catalogName == subProto.catalog_name(),
                  Status::INVALID_ARGUMENTS,
                  "catalog_name:[",
                  subProto.catalog_name(),
                  "] conflict with table:[",
                  id.databaseName,
                  ".",
                  id.tableName,
                  "] in catalog:[",
                  id.catalogName,
                  "]");
    return StatusBuilder::ok();
}

Status Table::fromProto(const proto::Table &proto) {
    _status = proto.status();
    _version = proto.version();
    return fromProtoOrCreate(proto, false);
}

template <typename T>
Status Table::toProto(T *proto, proto::DetailLevel::Code detailLevel) const {
    proto->mutable_status()->CopyFrom(_status);
    proto->set_version(_version);
    proto->set_table_name(_id.tableName);
    if (detailLevel == proto::DetailLevel::MINIMAL) {
        return StatusBuilder::ok();
    }
    proto->set_database_name(_id.databaseName);
    proto->set_catalog_name(_id.catalogName);
    proto->mutable_table_config()->CopyFrom(_tableConfig);
    auto nextDetailLevel = detailLevel == proto::DetailLevel::SUMMARY ? proto::DetailLevel::MINIMAL : detailLevel;
    if constexpr (std::is_same_v<std::decay_t<T>, proto::Table>) {
        CATALOG_CHECK_OK(_tableStructure->toProto(proto->mutable_table_structure(), nextDetailLevel));
    } else if constexpr (std::is_same_v<std::decay_t<T>, proto::TableRecord>) {
        auto record = proto->mutable_table_structure_record();
        record->set_entity_name(_id.tableName);
        record->set_version(_tableStructure->version());
    } else {
        static_assert(!sizeof(T), "unsupported proto type");
    }
    auto names = listPartition();
    std::sort(names.begin(), names.end());
    for (const auto &name : names) {
        const auto &partition = _partitions.at(name);
        if constexpr (std::is_same_v<std::decay_t<T>, proto::Table>) {
            CATALOG_CHECK_OK(partition->toProto(proto->add_partitions(), nextDetailLevel));
        } else if constexpr (std::is_same_v<std::decay_t<T>, proto::TableRecord>) {
            auto record = proto->add_partition_records();
            record->set_entity_name(name);
            record->set_version(partition->version());
        } else {
            static_assert(!sizeof(T), "unsupported proto type");
        }
    }
    proto->mutable_operation_meta()->CopyFrom(_operationMeta);
    return StatusBuilder::ok();
}

template Status Table::toProto<>(proto::Table *, proto::DetailLevel::Code) const;
template Status Table::toProto<>(proto::TableRecord *, proto::DetailLevel::Code) const;

Status Table::compare(const Table *other, DiffResult &diffResult) const {
    if (other != nullptr && other->version() == version()) {
        return StatusBuilder::ok();
    }
    for (const auto &it : partitions()) {
        const Partition *entity = nullptr;
        if (other != nullptr) {
            const auto &entities = other->partitions();
            auto otherId = entities.find(it.first);
            if (otherId != entities.end()) {
                entity = otherId->second.get();
            }
        }
        CATALOG_CHECK_OK(it.second->compare(entity, diffResult));
    }
    CATALOG_CHECK_OK(_tableStructure->compare(other == nullptr ? nullptr : other->tableStructure().get(), diffResult));

    auto proto = std::make_unique<proto::TableRecord>();
    CATALOG_CHECK_OK(toProto(proto.get()));
    diffResult.tables.emplace_back(std::move(proto));
    return StatusBuilder::ok();
}

void Table::updateChildStatus(const proto::EntityStatus &target, UpdateResult &updateResult) {
    for (const auto &[_, partition] : partitions()) {
        partition->updateStatus(target, updateResult);
    }
    _tableStructure->updateStatus(target, updateResult);
}

void Table::alignVersion(CatalogVersion version) {
    for (auto &it : partitions()) {
        it.second->alignVersion(version);
    }
    _tableStructure->alignVersion(version);
    if (_version == kToUpdateCatalogVersion) {
        _version = version;
    }
}

Status Table::create(const proto::Table &request) {
    if (request.partitions_size() > 0) {
        // 一次性创建Table及其Partitions，要求Partitions只能统一使用Table的TableStructure
        auto proto = request;
        CATALOG_CHECK(proto.has_table_structure(),
                      Status::UNSUPPORTED,
                      "table_structure for table:[",
                      _id.databaseName,
                      ".",
                      _id.tableName,
                      "] is not specified");
        {
            auto subProto = proto.mutable_table_structure();
            subProto->set_version(kToUpdateCatalogVersion);
            subProto->mutable_status()->set_code(proto::EntityStatus::PENDING_PUBLISH);
        }
        for (auto &subProto : *(proto.mutable_partitions())) {
            CATALOG_CHECK(!subProto.has_table_structure(),
                          Status::UNSUPPORTED,
                          "table_structure in partiton:[",
                          subProto.partition_name(),
                          "] should not be specified for table:[",
                          _id.databaseName,
                          ".",
                          _id.tableName,
                          "]");
            subProto.mutable_table_structure()->CopyFrom(proto.table_structure());
        }
        CATALOG_CHECK_OK(fromProtoOrCreate(proto, true));
    } else {
        CATALOG_CHECK_OK(fromProtoOrCreate(request, true));
    }
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Table::drop() {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_DELETE);
    return StatusBuilder::ok();
}

Status Table::update(const proto::Table &request) {
    bool tableStructureNoDiff = true;
    if (request.has_table_structure()) {
        if (_tableStructure == nullptr) {
            tableStructureNoDiff = false;
        } else {
            proto::TableStructure proto;
            CATALOG_CHECK_OK(_tableStructure->toProto(&proto));
            tableStructureNoDiff = ProtoUtil::compareProto(request.table_structure(), proto);
        }
    }
    CATALOG_CHECK(tableStructureNoDiff,
                  Status::UNSUPPORTED,
                  "table_structure for table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "] can't update directely, use 'updateTableStructure' interface instead");

    CATALOG_CHECK(!ProtoUtil::compareProto(_tableConfig, request.table_config()),
                  Status::INVALID_ARGUMENTS,
                  "table_config has no actual diff for table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    _tableConfig = request.table_config();
    _operationMeta = request.operation_meta();
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Table::updateTableStructure(const proto::TableStructure &request) {
    return execute([&](TableStructure &tableStructure) { return tableStructure.update(request); });
}

Status Table::addColumn(const proto::AddColumnRequest &request) {
    return execute([&](TableStructure &tableStructure) {
        CATALOG_CHECK(!request.columns().empty(), Status::INVALID_ARGUMENTS, "columns is not specified");
        for (const auto &column : request.columns()) {
            CATALOG_CHECK_OK(tableStructure.addColumn(column));
        }
        return StatusBuilder::ok();
    });
}

Status Table::updateColumn(const proto::UpdateColumnRequest &request) {
    return execute([&](TableStructure &tableStructure) {
        CATALOG_CHECK(request.has_column(), Status::INVALID_ARGUMENTS, "column is not specified");
        return tableStructure.updateColumn(request.column());
    });
}

Status Table::dropColumn(const proto::DropColumnRequest &request) {
    return execute([&](TableStructure &tableStructure) {
        CATALOG_CHECK(!request.column_names().empty(), Status::INVALID_ARGUMENTS, "column_names is not specified");
        for (const auto &columnName : request.column_names()) {
            CATALOG_CHECK_OK(tableStructure.dropColumn(columnName));
        }
        return StatusBuilder::ok();
    });
}

Status Table::createIndex(const proto::CreateIndexRequest &request) {
    return execute([&](TableStructure &tableStructure) {
        CATALOG_CHECK(!request.indexes().empty(), Status::INVALID_ARGUMENTS, "indexes is not specified");
        for (const auto &index : request.indexes()) {
            CATALOG_CHECK_OK(tableStructure.createIndex(index));
        }
        return StatusBuilder::ok();
    });
}

Status Table::updateIndex(const proto::UpdateIndexRequest &request) {
    return execute([&](TableStructure &tableStructure) {
        CATALOG_CHECK(request.has_index(), Status::INVALID_ARGUMENTS, "index is not specified");
        return tableStructure.updateIndex(request.index());
    });
}

Status Table::dropIndex(const proto::DropIndexRequest &request) {
    return execute([&](TableStructure &tableStructure) {
        CATALOG_CHECK(!request.index_names().empty(), Status::INVALID_ARGUMENTS, "index_names is not specified");
        for (const auto &indexName : request.index_names()) {
            CATALOG_CHECK_OK(tableStructure.dropIndex(indexName));
        }
        return StatusBuilder::ok();
    });
}

Status Table::createPartition(const proto::Partition &request) {
    CATALOG_CHECK_OK(addEntityCheck(request));
    const auto &partitionName = request.partition_name();
    CATALOG_CHECK(!request.has_table_structure(),
                  Status::UNSUPPORTED,
                  "table_structure in partiton:[",
                  partitionName,
                  "] should not be specified for table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    auto partition = std::make_unique<Partition>();
    {
        proto::Partition proto = request;
        CATALOG_CHECK_OK(_tableStructure->toProto(proto.mutable_table_structure()));
        CATALOG_CHECK_OK(partition->create(proto));
    }
    _partitions.try_emplace(partitionName, std::move(partition));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Table::dropPartition(const PartitionId &id) {
    return execute(id.partitionName, [&](Partition &partition) {
        CATALOG_CHECK_OK(partition.drop());
        _partitions.erase(id.partitionName);
        return StatusBuilder::ok();
    });
}

Status Table::updatePartition(const proto::Partition &request) {
    return execute(request.partition_name(), [&](Partition &partition) { return partition.update(request); });
}

Status Table::updatePartitionTableStructure(const proto::UpdatePartitionTableStructureRequest &request) {
    proto::TableStructure proto;
    CATALOG_CHECK_OK(_tableStructure->toProto(&proto));
    return execute(request.partition_name(),
                   [&](Partition &partition) { return partition.updateTableStructure(proto); });
}

Status Table::getPartition(const std::string &partitionName, Partition *&partition) const {
    auto it = _partitions.find(partitionName);
    CATALOG_CHECK(it != _partitions.end(),
                  Status::NOT_FOUND,
                  "partiton:[",
                  partitionName,
                  "] not exists in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
    partition = it->second.get();
    return StatusBuilder::ok();
}

bool Table::hasPartition(const std::string &partitionName) const {
    return _partitions.find(partitionName) != _partitions.end();
}

std::vector<std::string> Table::listPartition() const {
    std::vector<std::string> partitionNames;
    for (const auto &it : _partitions) {
        partitionNames.emplace_back(it.first);
    }
    return partitionNames;
}

Status Table::fromProtoOrCreate(const proto::Table &proto, bool create) {
    _id.tableName = proto.table_name();
    CATALOG_CHECK(!_id.tableName.empty(), Status::INVALID_ARGUMENTS, "table_name is not specified");
    _id.databaseName = proto.database_name();
    CATALOG_CHECK(!_id.databaseName.empty(), Status::INVALID_ARGUMENTS, "database_name is not specified");
    _id.catalogName = proto.catalog_name();
    CATALOG_CHECK(!_id.catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    _tableConfig = proto.table_config();
    {
        CATALOG_CHECK(proto.has_table_structure(),
                      Status::INVALID_ARGUMENTS,
                      "table_structure is not specified for table:[",
                      _id.databaseName,
                      ".",
                      _id.tableName,
                      "] with catalog:[",
                      _id.catalogName,
                      "]");
        const auto &subProto = proto.table_structure();
        CATALOG_CHECK_OK(checkSubProto(_id, subProto));
        auto tableStructure = std::make_unique<TableStructure>();
        if (create) {
            CATALOG_CHECK_OK(tableStructure->create(subProto));
        } else {
            CATALOG_CHECK_OK(tableStructure->fromProto(subProto));
        }
        _tableStructure = std::move(tableStructure);
    }
    for (const auto &subProto : proto.partitions()) {
        CATALOG_CHECK_OK(addEntityCheck(subProto));
        auto element = std::make_unique<Partition>();
        if (create) {
            CATALOG_CHECK_OK(element->create(subProto));
        } else {
            CATALOG_CHECK_OK(element->fromProto(subProto));
        }
        _partitions.try_emplace(subProto.partition_name(), std::move(element));
    }
    _operationMeta = proto.operation_meta();
    return StatusBuilder::ok();
}

Status Table::addEntityCheck(const proto::Partition &proto) {
    CATALOG_CHECK_OK(checkSubProto(_id, proto));
    CATALOG_CHECK(proto.partition_type() == proto::PartitionType::TABLE_PARTITION,
                  Status::INVALID_ARGUMENTS,
                  "invalid partition_type:[",
                  proto::PartitionType_Code_Name(proto.partition_type()),
                  "] for table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "] in catalog:[",
                  _id.catalogName,
                  "]");
    CATALOG_CHECK(_partitions.find(proto.partition_name()) == _partitions.end(),
                  Status::DUPLICATE_ENTITY,
                  "partition:[",
                  proto.partition_name(),
                  "] already exists in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "] with catalog:[",
                  _id.catalogName,
                  "]");
    return StatusBuilder::ok();
}

Status Table::execute(const std::string &partitionName, PartitionExec exec) {
    Partition *partition = nullptr;
    CATALOG_CHECK_OK(getPartition(partitionName, partition));
    CATALOG_CHECK_OK(exec(*partition));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Table::execute(TableStructureExec exec) {
    CATALOG_CHECK_OK(exec(*_tableStructure));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

} // namespace catalog
