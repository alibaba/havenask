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
#include <vector>

#include "indexlib/config/SortDescription.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueComparator.h"
#include "indexlib/index/kv/Record.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

namespace indexlibv2::index {

class RecordComparator
{
public:
    RecordComparator();
    ~RecordComparator();

public:
    bool Init(const std::shared_ptr<config::KVIndexConfig>& kvIndexConfig,
              const config::SortDescriptions& sortDescriptions);
    bool operator()(const Record& lfr, const Record& rhr) const;
    bool ValueEqual(const Record& lhs, const Record& rhs) const;
    bool ValueLessThan(const Record& lhs, const Record& rhs) const;

public:
    const PackValueComparator* TEST_GetValueComparator() const { return &_valueComparator; }

private:
    PackValueComparator _valueComparator;
};

} // namespace indexlibv2::index
