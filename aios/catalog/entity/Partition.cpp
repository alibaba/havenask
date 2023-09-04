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
#include "catalog/entity/Partition.h"

#include <google/protobuf/util/json_util.h>

#include "autil/legacy/md5.h"
#include "catalog/util/ProtoUtil.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, Partition);

std::string Partition::getSignature() const {
    catalog::proto::Partition proto;
    auto s = toProto(&proto);
    if (!isOk(s)) {
        return "";
    }
    std::string tmp;
    std::map<std::string, std::string> sortedMetas{proto.partition_config().custom_metas().begin(),
                                                   proto.partition_config().custom_metas().end()};
    for (const auto &iter : sortedMetas) {
        tmp += iter.first + ":" + iter.second;
    }

    proto.mutable_partition_config()->mutable_custom_metas()->clear();
    for (size_t i = 0; i < proto.table_structure().indexes_size(); i++) {
        auto index = proto.mutable_table_structure()->mutable_indexes(i);
        std::map<std::string, std::string> sortedIndexParam{index->index_config().index_params().begin(),
                                                            index->index_config().index_params().end()};
        for (const auto &iter : sortedIndexParam) {
            tmp += iter.first + ":" + iter.second;
        }
        index->mutable_index_config()->mutable_index_params()->clear();
    }
    std::string jsonStr;
    google::protobuf::util::MessageToJsonString(proto, &jsonStr);
    tmp += jsonStr;
    autil::legacy::Md5Stream stream;
    stream.Put((const uint8_t *)tmp.c_str(), tmp.length());
    return stream.GetMd5String();
}

void Partition::copyDetail(Partition *other) const {
    other->_partitionConfig = _partitionConfig;
    other->_dataSource = _dataSource;
    other->_tableStructure = _tableStructure == nullptr ? nullptr : _tableStructure->clone();
}

template <typename T>
inline Status checkSubProto(const PartitionId &id, const T &subProto) {
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

Status Partition::fromProto(const proto::Partition &proto) {
    _status = proto.status();
    _version = proto.version();
    return fromProtoOrCreate(proto);
}

template <typename T>
Status Partition::toProto(T *proto, proto::DetailLevel::Code detailLevel) const {
    proto->mutable_status()->CopyFrom(_status);
    proto->set_version(_version);
    proto->set_partition_name(_id.partitionName);
    if (detailLevel == proto::DetailLevel::MINIMAL) {
        return StatusBuilder::ok();
    }
    proto->set_table_name(_id.tableName);
    proto->set_database_name(_id.databaseName);
    proto->set_catalog_name(_id.catalogName);
    proto->set_partition_type(_id.partitionType);
    proto->mutable_partition_config()->CopyFrom(_partitionConfig);
    proto->mutable_data_source()->CopyFrom(_dataSource);
    if (_tableStructure != nullptr) {
        auto nextDetailLevel = detailLevel == proto::DetailLevel::SUMMARY ? proto::DetailLevel::MINIMAL : detailLevel;
        if constexpr (std::is_same_v<std::decay_t<T>, proto::Partition>) {
            CATALOG_CHECK_OK(_tableStructure->toProto(proto->mutable_table_structure(), nextDetailLevel));
        } else if constexpr (std::is_same_v<std::decay_t<T>, proto::PartitionRecord>) {
            auto record = proto->mutable_table_structure_record();
            record->set_entity_name(_id.tableName);
            record->set_version(_tableStructure->version());
        } else {
            static_assert(!sizeof(T), "unsupported proto type");
        }
    }
    proto->mutable_operation_meta()->CopyFrom(_operationMeta);
    return StatusBuilder::ok();
}

template Status Partition::toProto<>(proto::Partition *, proto::DetailLevel::Code) const;
template Status Partition::toProto<>(proto::PartitionRecord *, proto::DetailLevel::Code) const;

Status Partition::compare(const Partition *other, DiffResult &diffResult) const {
    if (other != nullptr && other->version() == version()) {
        return StatusBuilder::ok();
    }
    // Partition上的table_structure只能来自Table，其内容变化无须额外记录到Diff结构中
    auto proto = std::make_unique<proto::PartitionRecord>();
    CATALOG_CHECK_OK(toProto(proto.get()));
    diffResult.partitions.emplace_back(std::move(proto));
    return StatusBuilder::ok();
}

void Partition::updateChildStatus(const proto::EntityStatus &target, UpdateResult &result) {
    _tableStructure->updateStatus(target, result);
}

void Partition::alignVersion(CatalogVersion version) {
    if (_tableStructure != nullptr) {
        _tableStructure->alignVersion(version);
    }
    if (_version == kToUpdateCatalogVersion) {
        _version = version;
    }
}

Status Partition::create(const proto::Partition &request) {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return fromProtoOrCreate(request);
}

Status Partition::update(const proto::Partition &request) {
    CATALOG_CHECK(!request.has_data_source() || ProtoUtil::compareProto(request.data_source(), _dataSource),
                  Status::UNSUPPORTED,
                  "update for data_source in partition:[",
                  _id.partitionName,
                  "] is unsupported with table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");
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
                  "] partition:[",
                  _id.partitionName,
                  "] can't update directely, use 'updatePartitionTableStructure' interface instead");

    CATALOG_CHECK(!ProtoUtil::compareProto(_partitionConfig, request.partition_config()),
                  Status::INVALID_ARGUMENTS,
                  "partition_config has no actual diff for partition:[",
                  _id.partitionName,
                  "] in table:[",
                  _id.databaseName,
                  ".",
                  _id.tableName,
                  "]");

    _partitionConfig = request.partition_config();
    _operationMeta = request.operation_meta();
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Partition::updateTableStructure(const proto::TableStructure &request) {
    auto tableStructure = std::make_unique<TableStructure>();
    CATALOG_CHECK_OK(tableStructure->fromProto(request));
    _tableStructure = std::move(tableStructure);
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status Partition::drop() {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_DELETE);
    return StatusBuilder::ok();
}

Status Partition::fromProtoOrCreate(const proto::Partition &proto) {
    _id.partitionName = proto.partition_name();
    CATALOG_CHECK(!_id.partitionName.empty(), Status::INVALID_ARGUMENTS, "partition_name is not specified");
    _id.tableName = proto.table_name();
    CATALOG_CHECK(!_id.tableName.empty(), Status::INVALID_ARGUMENTS, "table_name is not specified");
    _id.databaseName = proto.database_name();
    CATALOG_CHECK(!_id.databaseName.empty(), Status::INVALID_ARGUMENTS, "database_name is not specified");
    _id.catalogName = proto.catalog_name();
    CATALOG_CHECK(!_id.catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");
    _id.partitionType = proto.partition_type();
    CATALOG_CHECK(_id.partitionType == proto::PartitionType::TABLE_PARTITION,
                  Status::INVALID_ARGUMENTS,
                  "invalid partition_type:[",
                  proto::PartitionType_Code_Name(_id.partitionType),
                  "]");
    _partitionConfig = proto.partition_config();
    _dataSource = proto.data_source();
    if (proto.has_table_structure()) {
        const auto &subProto = proto.table_structure();
        CATALOG_CHECK_OK(checkSubProto(_id, subProto));
        auto tableStructure = std::make_unique<TableStructure>();
        // create的partition，其tableStructure的状态由外部输入决定
        CATALOG_CHECK_OK(tableStructure->fromProto(subProto));
        _tableStructure = std::move(tableStructure);
    }
    _operationMeta = proto.operation_meta();
    return StatusBuilder::ok();
}

int32_t Partition::shardCount() const { return _tableStructure->shardInfo().shard_count(); }

} // namespace catalog
