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

#include "catalog/entity/Type.h"
#include "catalog/entity/UpdateResult.h"

namespace catalog {

template <typename EntityType>
struct EntityTraits {};

#define SPECIALIZE_TRAITS(ClassType, EntityIdType)                                                                     \
    class ClassType;                                                                                                   \
    template <>                                                                                                        \
    struct EntityTraits<ClassType> {                                                                                   \
        using IdType = EntityIdType;                                                                                   \
    }

template <typename Self>
class EntityBase {
public:
    using IdType = typename EntityTraits<Self>::IdType;

public:
    virtual ~EntityBase() = default;

public:
    void updateStatus(const proto::EntityStatus &target, UpdateResult &result) {
        self()->updateChildStatus(target, result);
        if (_status.code() != target.code()) {
            _status.set_code(target.code());
            result.addDiff(_id, _version, _status);
        }
    }
    const proto::EntityStatus &status() const { return _status; }

    void updateVersion(CatalogVersion version) { _version = version; }
    CatalogVersion version() const { return _version; }

    const IdType &id() const { return _id; }
    IdType &id() { return _id; }

    const auto &operationMeta() const { return _operationMeta; }

    std::unique_ptr<Self> clone() const {
        auto ret = std::make_unique<Self>();
        copyBase(ret.get());
        self()->copyDetail(ret.get());
        return ret;
    }

private:
    void copyBase(Self *entity) const {
        entity->_version = _version;
        entity->_status = _status;
        entity->_id = _id;
        entity->_operationMeta = _operationMeta;
    }

private:
    const Self *self() const { return static_cast<const Self *>(this); }
    Self *self() { return static_cast<Self *>(this); }

protected:
    CatalogVersion _version = kInvalidCatalogVersion;
    proto::EntityStatus _status;
    IdType _id;
    // TODO: remove this field
    proto::OperationMeta _operationMeta;
};

} // namespace catalog
