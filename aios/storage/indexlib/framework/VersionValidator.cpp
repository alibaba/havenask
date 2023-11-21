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
#include "indexlib/framework/VersionValidator.h"

#include <assert.h>

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/ITabletValidator.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/TabletFactoryCreator.h"
#include "indexlib/framework/TabletSchemaLoader.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, VersionValidator);

Status VersionValidator::Validate(const std::string& indexRootPath,
                                  const std::shared_ptr<const config::ITabletSchema>& schema, versionid_t versionId)
{
    AUTIL_LOG(INFO, "version validate begin");
    std::string content;
    if (!schema->Serialize(false, &content)) {
        RETURN_STATUS_ERROR(Corruption, "serialize tablet schema failed");
    }
    std::shared_ptr<config::TabletSchema> cloneSchema = TabletSchemaLoader::LoadSchema(content);
    if (!cloneSchema) {
        assert(false);
        RETURN_STATUS_ERROR(Corruption, "de-serialize tablet schema failed [%s]", content.c_str());
    }

    auto status = TabletSchemaLoader::ResolveSchema(nullptr /*options*/, indexRootPath, cloneSchema.get());
    RETURN_IF_STATUS_ERROR(status, "resolve clone schema failed");

    auto metricsManager = std::make_shared<framework::MetricsManager>("", nullptr);
    auto tabletFactory = TabletFactoryCreator::GetInstance()->Create(cloneSchema->GetTableType());
    if (!tabletFactory ||
        !tabletFactory->Init(std::make_shared<indexlibv2::config::TabletOptions>(), metricsManager.get())) {
        RETURN_STATUS_ERROR(Corruption, "init factory failed");
    }

    auto tabletValidator = tabletFactory->CreateTabletValidator();
    status = tabletValidator->Validate(indexRootPath, cloneSchema, versionId);
    RETURN_IF_STATUS_ERROR(status, "validate tablet[%s] on version[%d] failed", indexRootPath.c_str(), versionId);

    AUTIL_LOG(INFO, "version validate end, tablet[%s] on version[%d] is valid", indexRootPath.c_str(), versionId);
    return Status::OK();
}

} // namespace indexlibv2::framework
