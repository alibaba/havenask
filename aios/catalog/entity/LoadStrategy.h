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

#include "autil/Log.h"
#include "catalog/entity/DiffResult.h"
#include "catalog/entity/EntityBase.h"
#include "catalog/entity/Type.h"
#include "catalog/entity/UpdateResult.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

SPECIALIZE_TRAITS(LoadStrategy, LoadStrategyId);

class LoadStrategy : public EntityBase<LoadStrategy> {
public:
    LoadStrategy() = default;
    ~LoadStrategy() = default;
    LoadStrategy(const LoadStrategy &) = delete;
    LoadStrategy &operator=(const LoadStrategy &) = delete;

public:
    const auto &loadStrategyConfig() const { return _loadStrategyConfig; }

    void copyDetail(LoadStrategy *other) const;
    Status fromProto(const proto::LoadStrategy &proto);
    Status toProto(proto::LoadStrategy *proto,
                   proto::DetailLevel::Code detailLevel = proto::DetailLevel::DETAILED) const;
    Status compare(const LoadStrategy *other, DiffResult &diffResult) const;
    void updateChildStatus(const proto::EntityStatus &target, UpdateResult &result);
    void alignVersion(CatalogVersion version);

    Status create(const proto::LoadStrategy &request);
    Status update(const proto::LoadStrategy &request);
    Status drop();

private:
    Status fromProtoOrCreate(const proto::LoadStrategy &proto);

private:
    proto::LoadStrategyConfig _loadStrategyConfig;
    AUTIL_LOG_DECLARE();
};

} // namespace catalog
