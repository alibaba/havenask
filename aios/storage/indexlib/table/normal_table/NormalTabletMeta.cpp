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
#include "indexlib/table/normal_table/NormalTabletMeta.h"

#include "indexlib/base/Constant.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletMeta);

NormalTabletMeta::NormalTabletMeta(const config::SortDescriptions& sortDescs) : _sortDescriptions(sortDescs) {}

config::SortPattern NormalTabletMeta::GetSortPattern(const std::string& sortField) const
{
    for (size_t i = 0; i < _sortDescriptions.size(); ++i) {
        if (_sortDescriptions[i].GetSortFieldName() == sortField) {
            return _sortDescriptions[i].GetSortPattern();
        }
    }
    return config::sp_nosort;
}

const config::SortDescription& NormalTabletMeta::GetSortDescription(size_t idx) const
{
    assert(idx < _sortDescriptions.size());
    return _sortDescriptions[idx];
}

const config::SortDescriptions& NormalTabletMeta::GetSortDescriptions() const { return _sortDescriptions; }
size_t NormalTabletMeta::Size() const { return _sortDescriptions.size(); }

} // namespace indexlibv2::table
