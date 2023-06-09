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
#include "indexlib/table/orc_table/OrcTableFactory.h"

#include "indexlib/config/OnlineConfig.h"
#include "indexlib/document/orc/OrcDocumentFactory.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/table/orc_table/OrcSchemaResolver.h"
#include "indexlib/table/orc_table/OrcTabletLoader.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, OrcTableFactory);

std::unique_ptr<framework::ITabletFactory> OrcTableFactory::Create() const
{
    return std::make_unique<OrcTableFactory>();
}

std::unique_ptr<config::SchemaResolver> OrcTableFactory::CreateSchemaResolver() const
{
    return std::make_unique<OrcSchemaResolver>();
}

std::unique_ptr<framework::ITabletLoader> OrcTableFactory::CreateTabletLoader(const std::string& branchName)
{
    bool loadIndexForCheck = false;
    if (!GetTabletOptions()->GetFromRawJson("load_index_for_check", &loadIndexForCheck)) {
        loadIndexForCheck = false;
    }
    return std::make_unique<OrcTabletLoader>(
        std::move(branchName), GetTabletOptions()->GetOnlineConfig().IsIncConsistentWithRealtime(), loadIndexForCheck);
}

std::unique_ptr<document::IDocumentFactory>
OrcTableFactory::CreateDocumentFactory(const std::shared_ptr<config::TabletSchema>& schema)
{
    return std::make_unique<indexlib::document::OrcDocumentFactory>();
}

REGISTER_FACTORY(orc, OrcTableFactory);

} // namespace indexlibv2::table
