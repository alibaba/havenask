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
#include "catalog/entity/TableGroup.h"

#include "catalog/util/ProtoUtil.h"

namespace catalog {
AUTIL_LOG_SETUP(catalog, TableGroup);

void TableGroup::copyDetail(TableGroup *other) const {
    other->_tableGroupConfig = _tableGroupConfig;
    for (const auto &it : _loadStrategies) {
        other->_loadStrategies.emplace(it.first, it.second->clone());
    }
}

Status TableGroup::fromProto(const proto::TableGroup &proto) {
    _status = proto.status();
    _version = proto.version();
    return fromProtoOrCreate(proto, false);
}

template <typename T>
Status TableGroup::toProto(T *proto, proto::DetailLevel::Code detailLevel) const {
    proto->mutable_status()->CopyFrom(_status);
    proto->set_version(_version);
    proto->set_table_group_name(_id.tableGroupName);
    if (detailLevel == proto::DetailLevel::MINIMAL) {
        return StatusBuilder::ok();
    }
    proto->set_database_name(_id.databaseName);
    proto->set_catalog_name(_id.catalogName);
    proto->mutable_table_group_config()->CopyFrom(_tableGroupConfig);
    auto nextDetailLevel = detailLevel == proto::DetailLevel::SUMMARY ? proto::DetailLevel::MINIMAL : detailLevel;
    auto names = listLoadStrategy();
    std::sort(names.begin(), names.end());
    for (const auto &name : names) {
        auto it = _loadStrategies.find(name);
        CATALOG_CHECK(it != _loadStrategies.end(), Status::INTERNAL_ERROR);
        if constexpr (std::is_same_v<std::decay_t<T>, proto::TableGroup>) {
            CATALOG_CHECK_OK(it->second->toProto(proto->add_load_strategies(), nextDetailLevel));
        } else if constexpr (std::is_same_v<std::decay_t<T>, proto::TableGroupRecord>) {
            auto record = proto->add_load_strategy_records();
            record->set_entity_name(name);
            record->set_version(it->second->version());
        } else {
            static_assert(!sizeof(T), "unsupported proto type");
        }
    }
    proto->mutable_operation_meta()->CopyFrom(_operationMeta);
    return StatusBuilder::ok();
}

template Status TableGroup::toProto<>(proto::TableGroup *, proto::DetailLevel::Code) const;
template Status TableGroup::toProto<>(proto::TableGroupRecord *, proto::DetailLevel::Code) const;

Status TableGroup::compare(const TableGroup *other, DiffResult &diffResult) const {
    if (other != nullptr && other->version() == version()) {
        return StatusBuilder::ok();
    }
    for (const auto &it : loadStrategies()) {
        const LoadStrategy *entity = nullptr;
        if (other != nullptr) {
            const auto &entities = other->loadStrategies();
            auto otherId = entities.find(it.first);
            if (otherId != entities.end()) {
                entity = otherId->second.get();
            }
        }
        CATALOG_CHECK_OK(it.second->compare(entity, diffResult));
    }

    auto proto = std::make_unique<proto::TableGroupRecord>();
    CATALOG_CHECK_OK(toProto(proto.get()));
    diffResult.tableGroups.emplace_back(std::move(proto));
    return StatusBuilder::ok();
}

void TableGroup::updateChildStatus(const proto::EntityStatus &target, UpdateResult &updateResult) {
    for (const auto &[_, loadStrategy] : loadStrategies()) {
        loadStrategy->updateStatus(target, updateResult);
    }
}

void TableGroup::alignVersion(CatalogVersion version) {
    for (auto &it : loadStrategies()) {
        it.second->alignVersion(version);
    }
    if (_version == kToUpdateCatalogVersion) {
        _version = version;
    }
}

Status TableGroup::create(const proto::TableGroup &request) {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return fromProtoOrCreate(request, true);
}

Status TableGroup::update(const proto::TableGroup &request) {
    CATALOG_CHECK(!ProtoUtil::compareProto(_tableGroupConfig, request.table_group_config()),
                  Status::INVALID_ARGUMENTS,
                  "table_group_config has no actual diff for table_group:[",
                  _id.databaseName,
                  ".",
                  _id.tableGroupName,
                  "]");
    _tableGroupConfig = request.table_group_config();
    _operationMeta = request.operation_meta();
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status TableGroup::drop() {
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_DELETE);
    return StatusBuilder::ok();
}

Status TableGroup::createLoadStrategy(const proto::LoadStrategy &request) {
    CATALOG_CHECK_OK(addEntityCheck(request));
    auto element = std::make_unique<LoadStrategy>();
    CATALOG_CHECK_OK(element->create(request));
    _loadStrategies.try_emplace(request.table_name(), std::move(element));

    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

Status TableGroup::updateLoadStrategy(const proto::LoadStrategy &request) {
    return execute(request.table_name(), [&](LoadStrategy &loadStrategy) { return loadStrategy.update(request); });
}

Status TableGroup::dropLoadStrategy(const LoadStrategyId &id) {
    return execute(id.tableName, [&](LoadStrategy &loadStrategy) {
        CATALOG_CHECK_OK(loadStrategy.drop());
        _loadStrategies.erase(id.tableName);
        return StatusBuilder::ok();
    });
}

Status TableGroup::getLoadStrategy(const std::string &tableName, LoadStrategy *&loadStrategy) const {
    auto it = _loadStrategies.find(tableName);
    CATALOG_CHECK(it != _loadStrategies.end(),
                  Status::NOT_FOUND,
                  "load_strategy for table:[",
                  tableName,
                  "] not exists in table_group:[",
                  _id.databaseName,
                  ".",
                  _id.tableGroupName,
                  "]");
    loadStrategy = it->second.get();
    return StatusBuilder::ok();
}

bool TableGroup::hasLoadStrategy(const std::string &tableName) const {
    return _loadStrategies.find(tableName) != _loadStrategies.end();
}

std::vector<std::string> TableGroup::listLoadStrategy() const {
    std::vector<std::string> loadStrategyNames;
    for (const auto &it : _loadStrategies) {
        loadStrategyNames.emplace_back(it.first);
    }
    return loadStrategyNames;
}

Status TableGroup::fromProtoOrCreate(const proto::TableGroup &proto, bool create) {
    _id.tableGroupName = proto.table_group_name();
    CATALOG_CHECK(!_id.tableGroupName.empty(), Status::INVALID_ARGUMENTS, "table_group_name is not specified");
    _id.databaseName = proto.database_name();
    CATALOG_CHECK(!_id.databaseName.empty(), Status::INVALID_ARGUMENTS, "database_name is not specified");
    _id.catalogName = proto.catalog_name();
    CATALOG_CHECK(!_id.catalogName.empty(), Status::INVALID_ARGUMENTS, "catalog_name is not specified");

    _tableGroupConfig = proto.table_group_config();
    for (const auto &subProto : proto.load_strategies()) {
        CATALOG_CHECK_OK(addEntityCheck(subProto));
        auto element = std::make_unique<LoadStrategy>();
        if (create) {
            CATALOG_CHECK_OK(element->create(subProto));
        } else {
            CATALOG_CHECK_OK(element->fromProto(subProto));
        }
        _loadStrategies.try_emplace(subProto.table_name(), std::move(element));
    }
    _operationMeta = proto.operation_meta();
    return StatusBuilder::ok();
}

Status TableGroup::addEntityCheck(const proto::LoadStrategy &proto) {
    CATALOG_CHECK(_id.tableGroupName == proto.table_group_name(),
                  Status::INVALID_ARGUMENTS,
                  "table_group_name:[",
                  proto.table_name(),
                  "] conflict with table_group:[",
                  _id.databaseName,
                  ".",
                  _id.tableGroupName,
                  "] in catalog:[",
                  _id.catalogName,
                  "]");
    CATALOG_CHECK(_id.databaseName == proto.database_name(),
                  Status::INVALID_ARGUMENTS,
                  "database_name:[",
                  proto.database_name(),
                  "] conflict with table_group:[",
                  _id.databaseName,
                  ".",
                  _id.tableGroupName,
                  "] in catalog:[",
                  _id.catalogName,
                  "]");
    CATALOG_CHECK(_id.catalogName == proto.catalog_name(),
                  Status::INVALID_ARGUMENTS,
                  "catalog_name:[",
                  proto.catalog_name(),
                  "] conflict with table_group:[",
                  _id.databaseName,
                  ".",
                  _id.tableGroupName,
                  "] in catalog:[",
                  _id.catalogName,
                  "]");

    const auto &name = proto.table_name();
    CATALOG_CHECK(_loadStrategies.find(name) == _loadStrategies.end(),
                  Status::DUPLICATE_ENTITY,
                  "load_strategy for table:[",
                  name,
                  "] already exists in table_group:[",
                  _id.databaseName,
                  ".",
                  _id.tableGroupName,
                  "] with catalog:[",
                  _id.catalogName,
                  "]");
    return StatusBuilder::ok();
}

Status TableGroup::execute(const std::string &tableName, LoadStrategyExec exec) {
    LoadStrategy *loadStrategy = nullptr;
    CATALOG_CHECK_OK(getLoadStrategy(tableName, loadStrategy));
    CATALOG_CHECK_OK(exec(*loadStrategy));
    _version = kToUpdateCatalogVersion;
    _status.set_code(proto::EntityStatus::PENDING_PUBLISH);
    return StatusBuilder::ok();
}

} // namespace catalog
