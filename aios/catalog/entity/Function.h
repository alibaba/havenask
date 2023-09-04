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

#include <functional>
#include <string>

#include "autil/Log.h"
#include "catalog/entity/DiffResult.h"
#include "catalog/entity/EntityBase.h"
#include "catalog/entity/Type.h"
#include "catalog/entity/UpdateResult.h"
#include "catalog/util/StatusBuilder.h"

namespace catalog {

SPECIALIZE_TRAITS(Function, FunctionId);

class Function : public EntityBase<Function> {
public:
    Function() = default;
    ~Function() = default;
    Function(const Function &) = delete;
    Function &operator=(const Function &) = delete;

public:
    const auto &functionConfig() const { return _functionConfig; }

    void copyDetail(Function *other) const;
    Status fromProto(const proto::Function &proto);
    Status toProto(proto::Function *proto, proto::DetailLevel::Code detailLevel = proto::DetailLevel::DETAILED) const;
    Status compare(const Function *other, DiffResult &diffResult) const;
    void updateChildStatus(const proto::EntityStatus &target, UpdateResult &result);
    void alignVersion(CatalogVersion version);

    Status create(const proto::Function &request);
    Status update(const proto::Function &request);
    Status drop();

private:
    Status fromProtoOrCreate(const proto::Function &proto);

private:
    proto::FunctionConfig _functionConfig;
    AUTIL_LOG_DECLARE();
};

} // namespace catalog
