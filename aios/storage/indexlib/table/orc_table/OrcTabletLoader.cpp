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
#include "indexlib/table/orc_table/OrcTabletLoader.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/table/BuiltinDefine.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, OrcTabletLoader);
#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

OrcTabletLoader::OrcTabletLoader(const std::string& fenceName, bool reopenDataConsistent, bool loadIndexForCheck)
    : NormalTabletLoader(fenceName, reopenDataConsistent, loadIndexForCheck)
{
}

OrcTabletLoader::~OrcTabletLoader() {}
Status OrcTabletLoader::DoPreLoad(const framework::TabletData& lastTabletData,
                                  std::vector<std::shared_ptr<framework::Segment>> newOnDiskVersionSegments,
                                  const framework::Version& newOnDiskVersion)
{
    assert(_schema);
    assert(_schema->GetTableType() == indexlib::table::TABLE_TYPE_ORC);
    return CommonTabletLoader::DoPreLoad(lastTabletData, newOnDiskVersionSegments, newOnDiskVersion);
}

std::pair<Status, std::unique_ptr<framework::TabletData>>
OrcTabletLoader::FinalLoad(const framework::TabletData& currentTabletData)
{
    TABLET_LOG(INFO, "final load begin");
    assert(_schema->GetTableType() == indexlib::table::TABLE_TYPE_ORC);
    return CommonTabletLoader::FinalLoad(currentTabletData);
}
} // namespace indexlibv2::table
