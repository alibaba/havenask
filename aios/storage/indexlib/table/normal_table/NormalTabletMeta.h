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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/SortDescription.h"

namespace indexlibv2::table {
class NormalTabletMeta
{
public:
    explicit NormalTabletMeta(const config::SortDescriptions& sortDescs);

public:
    const config::SortDescription& GetSortDescription(size_t idx) const;
    config::SortPattern GetSortPattern(const std::string& sortField) const;

    const config::SortDescriptions& GetSortDescriptions() const;
    size_t Size() const;

private:
    config::SortDescriptions _sortDescriptions;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
