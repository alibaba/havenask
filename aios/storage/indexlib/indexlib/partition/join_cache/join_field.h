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

#include <map>
#include <memory>

#include "indexlib/indexlib.h"

namespace indexlib { namespace partition {

extern const std::string BUILD_IN_JOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX;
extern const std::string BUILD_IN_SUBJOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX;

struct JoinField {
    JoinField() {}
    JoinField(const std::string& _fieldName, const std::string& _joinTableName, bool _genJoinCache, bool _isStrongJoin)
        : genJoinCache(_genJoinCache)
        , isStrongJoin(_isStrongJoin)
        , fieldName(_fieldName)
        , joinTableName(_joinTableName)
    {
    }
    bool operator==(const JoinField& other) const
    {
        return genJoinCache == other.genJoinCache && isStrongJoin == other.isStrongJoin &&
               fieldName == other.fieldName && joinTableName == other.joinTableName;
    }

    static std::string GenerateVirtualAttributeName(const std::string& prefixName, uint64_t auxPartitionIdentifier,
                                                    const std::string& joinField, versionid_t vertionid);

    bool genJoinCache;
    bool isStrongJoin;
    std::string fieldName;
    std::string joinTableName;
};

std::ostream& operator<<(std::ostream& os, const JoinField& joinField);

// tableName --> JoinFields
typedef std::map<std::string, std::vector<JoinField>> JoinRelationMap;
typedef std::map<std::string, JoinField> JoinFieldMap;
// ReverseJoinRelationMap[auxTableName][mainTableName] --> JoinField
typedef std::map<std::string, JoinFieldMap> ReverseJoinRelationMap;
}} // namespace indexlib::partition
