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
#include "indexlib/config/table_identifier.h"

#include "indexlib/config/impl/table_identifier_impl.h"

using namespace std;

namespace indexlib { namespace config {

TableIdentifier::TableIdentifier(const string& id, TableType type, const string& name,
                                 TableIdentifier::Partition partition)
    : mImpl(new TableIdentifierImpl(id, type, name, partition))
{
}

void TableIdentifier::SetId(const std::string& id) { mImpl->mId = id; }

void TableIdentifier::SetTableName(const std::string& name) { mImpl->mName = name; }

void TableIdentifier::SetTableType(TableType type) { mImpl->mType = type; }

void TableIdentifier::SetPartition(const TableIdentifier::Partition& partition) { mImpl->mPartition = partition; }

const std::string& TableIdentifier::GetId() const { return mImpl->mId; }

const std::string& TableIdentifier::GetTableName() const { return mImpl->mName; }

TableType TableIdentifier::GetTableType() const { return mImpl->mType; }

const TableIdentifier::Partition& TableIdentifier::GetPartition() const { return mImpl->mPartition; }
}} // namespace indexlib::config
