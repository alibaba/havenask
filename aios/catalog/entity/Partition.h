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

#include <memory>
#include <string>

#include "autil/Log.h"
#include "catalog/entity/DiffResult.h"
#include "catalog/entity/EntityBase.h"
#include "catalog/entity/TableStructure.h"
#include "catalog/entity/Type.h"
#include "catalog/entity/UpdateResult.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

SPECIALIZE_TRAITS(Partition, PartitionId);

class Partition : public EntityBase<Partition> {
public:
    Partition() = default;
    ~Partition() = default;
    Partition(const Partition &) = delete;
    Partition &operator=(const Partition &) = delete;

public:
    const auto &partitionConfig() const { return _partitionConfig; }
    const auto &dataSource() const { return _dataSource; }
    const auto &tableStructure() const { return _tableStructure; }
    const auto &partitionName() const { return _id.partitionName; }
    const auto &tableName() const { return _id.tableName; }
    const TableStructure *getTableStructure() const { return _tableStructure.get(); }
    std::string getSignature() const;

    void copyDetail(Partition *other) const;
    Status fromProto(const proto::Partition &proto);
    template <typename T>
    Status toProto(T *proto, proto::DetailLevel::Code detailLevel = proto::DetailLevel::DETAILED) const;
    Status compare(const Partition *other, DiffResult &diffResult) const;
    void updateChildStatus(const proto::EntityStatus &target, UpdateResult &result);
    void alignVersion(CatalogVersion version);

    Status create(const proto::Partition &request);
    Status update(const proto::Partition &request);
    Status updateTableStructure(const proto::TableStructure &request);
    Status drop();
    int32_t shardCount() const;

private:
    Status fromProtoOrCreate(const proto::Partition &proto);

private:
    proto::PartitionConfig _partitionConfig;
    proto::DataSource _dataSource;
    std::unique_ptr<TableStructure> _tableStructure;
    AUTIL_LOG_DECLARE();
};

extern template Status Partition::toProto<>(proto::Partition *, proto::DetailLevel::Code) const;
extern template Status Partition::toProto<>(proto::PartitionRecord *, proto::DetailLevel::Code) const;

} // namespace catalog
