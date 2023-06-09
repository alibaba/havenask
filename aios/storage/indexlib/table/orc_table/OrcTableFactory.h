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

#include "autil/Log.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/table/normal_table/NormalTableFactory.h"

namespace indexlibv2::table {

class OrcTableFactory : public NormalTableFactory
{
public:
    std::unique_ptr<ITabletFactory> Create() const override;

    std::unique_ptr<config::SchemaResolver> CreateSchemaResolver() const override;

    std::unique_ptr<framework::ITabletLoader> CreateTabletLoader(const std::string& branchName) override;

    std::unique_ptr<document::IDocumentFactory>
    CreateDocumentFactory(const std::shared_ptr<config::TabletSchema>& schema) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
