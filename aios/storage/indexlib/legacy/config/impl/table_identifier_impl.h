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

#include "indexlib/config/table_identifier.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class TableIdentifierImpl
{
private:
    friend class TableIdentifier;

public:
    TableIdentifierImpl(const std::string& id, TableType type, const std::string& name,
                        TableIdentifier::Partition partition)
        : mType(type)
        , mId(id)
        , mName(name)
        , mPartition(partition)
    {
    }
    ~TableIdentifierImpl() {}

private:
    TableType mType;
    std::string mId;
    std::string mName;
    TableIdentifier::Partition mPartition;
};

typedef std::shared_ptr<TableIdentifierImpl> TableIdentifierImplPtr;
}} // namespace indexlib::config
