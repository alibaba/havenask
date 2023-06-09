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
#include "indexlib/partition/join_cache/join_field.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace partition {

const std::string BUILD_IN_JOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX = "_@_join_docid_";
const std::string BUILD_IN_SUBJOIN_DOCID_VIRTUAL_ATTR_NAME_PREFIX = "_@_subjoin_docid_";

std::string JoinField::GenerateVirtualAttributeName(const string& prefixName, uint64_t auxPartitionIdentifier,
                                                    const string& joinField, versionid_t vertionid)
{
    return prefixName + joinField + "_" + StringUtil::toString(auxPartitionIdentifier) + "_" +
           StringUtil::toString(vertionid);
}

std::ostream& operator<<(std::ostream& os, const JoinField& joinField)
{
    return os << "(field name:" << joinField.fieldName << ", join table: " << joinField.joinTableName
              << ", is strong join: " << joinField.isStrongJoin;
}

}} // namespace indexlib::partition
