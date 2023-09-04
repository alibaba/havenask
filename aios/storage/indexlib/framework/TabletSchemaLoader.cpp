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
#include "indexlib/framework/TabletSchemaLoader.h"

#include "autil/StringUtil.h"
#include "indexlib/config/SchemaResolver.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/TabletFactoryCreator.h"

using namespace indexlib::file_system;

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletSchemaLoader);

namespace {
struct SchemaFileComp {
    bool operator()(const std::string& lft, const std::string& rht)
    {
        schemaid_t lftVal = TabletSchemaLoader::GetSchemaId(lft);
        schemaid_t rhtVal = TabletSchemaLoader::GetSchemaId(rht);
        return lftVal < rhtVal;
    }
};
} // namespace

Status TabletSchemaLoader::ListSchema(const std::shared_ptr<IDirectory>& dir, fslib::FileList* fileList)
{
    Status st = dir->ListDir("", ListOption(), *fileList).Status();
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "list dir failed, logical dir[%s] error[%s]", dir->GetOutputPath().c_str(),
                  st.ToString().c_str());
        return st;
    }

    fslib::FileList::iterator it = std::remove_if(fileList->begin(), fileList->end(), IsNotSchemaFile);
    fileList->erase(it, fileList->end());
    std::sort(fileList->begin(), fileList->end(), SchemaFileComp());
    return Status::OK();
}

std::unique_ptr<config::TabletSchema> TabletSchemaLoader::GetSchema(const std::shared_ptr<IDirectory>& dir,
                                                                    schemaid_t schemaId)
{
    std::string schemaFileName = config::TabletSchema::GetSchemaFileName(schemaId);
    return LoadSchema(dir, schemaFileName);
}

std::unique_ptr<config::TabletSchema> TabletSchemaLoader::LoadSchema(const std::string& dir,
                                                                     const std::string& schemaFileName)
{
    std::string jsonStr;
    std::string physicalPath = dir + "/" + schemaFileName;
    if (!indexlib::file_system::FslibWrapper::AtomicLoad(physicalPath, jsonStr).OK()) {
        AUTIL_LOG(ERROR, "load schema file [%s] failed", physicalPath.c_str());
        return nullptr;
    }
    return LoadSchema(jsonStr);
}

std::unique_ptr<config::TabletSchema> TabletSchemaLoader::LoadSchema(const std::string& schemaStr)
{
    auto schema = TabletSchemaDelegation::Deserialize(schemaStr);
    if (!schema) {
        AUTIL_LOG(ERROR, "deserialize schema failed, [%s]", schemaStr.c_str());
        return nullptr;
    }
    const auto& tableType = schema->GetTableType();
    // TODO: remove when indexlibv2 support these table type
    if (tableType == "raw_file" || tableType == "line_data") {
        return schema;
    }

    auto options = std::make_shared<config::TabletOptions>();
    const auto& tableName = schema->GetTableName();
    auto tabletFactory = CreateTabletFactory(tableType, options);
    if (!tabletFactory) {
        if (tableType == "customized") {
            AUTIL_LOG(INFO, "No tablet factory defined for customized table[%s], parse schema as legacy schema",
                      tableName.c_str());
            return schema;
        }
        AUTIL_LOG(ERROR, "create tablet factory failed, table_name[%s] table_type[%s]", tableName.c_str(),
                  tableType.c_str());
        return nullptr;
    }
    auto schemaResolver = tabletFactory->CreateSchemaResolver();
    if (!schemaResolver) {
        AUTIL_LOG(ERROR, "create schema resolver failed, table_name[%s] table_type[%s]", tableName.c_str(),
                  tableType.c_str());
        return nullptr;
    }
    auto legacySchema = schema->GetLegacySchema();
    if (legacySchema) {
        auto status =
            TabletSchemaDelegation::LegacySchemaToTabletSchema(schemaResolver.get(), *legacySchema, schema.get());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "legacy schema to tablet schema failed, table_name[%s] table_type[%s]", tableName.c_str(),
                      tableType.c_str());
            return nullptr;
        }
    }
    return schema;
}

std::unique_ptr<config::TabletSchema> TabletSchemaLoader::LoadSchema(const std::shared_ptr<IDirectory>& dir,
                                                                     const std::string& schemaFileName)
{
    AUTIL_LOG(INFO, "begin load schema, name [%s], dir[%s]", schemaFileName.c_str(), dir->DebugString().c_str());
    std::string content;
    if (!dir->Load(schemaFileName, ReaderOption(FSOT_MEM), content).OK()) {
        AUTIL_LOG(ERROR, "load schema file failed, logical dir[%s] schema_name[%s]", dir->DebugString().c_str(),
                  schemaFileName.c_str());
        return nullptr;
    }
    auto schema = LoadSchema(content);
    if (!schema) {
        AUTIL_LOG(ERROR, "load schema failed, name[%s]", schemaFileName.c_str());
    } else {
        AUTIL_LOG(INFO, "end load schema, name [%s]", schemaFileName.c_str());
    }
    return schema;
}

Status TabletSchemaLoader::ResolveSchema(const std::shared_ptr<config::TabletOptions>& options,
                                         const std::string& indexPath, config::ITabletSchema* ischema)
{
    auto* schema = dynamic_cast<config::TabletSchema*>(ischema);
    if (!schema) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "schema is nullptr");
    }
    const auto& tableType = schema->GetTableType();
    // TODO: remove when indexlibv2 support these table type
    if (tableType == "raw_file" || tableType == "line_data" || tableType == "customized") {
        return Status::OK();
    }
    const auto& tableName = schema->GetTableName();
    std::shared_ptr<config::TabletOptions> tabletOptions = options;
    if (!tabletOptions) {
        tabletOptions = std::make_shared<config::TabletOptions>();
    }
    auto tabletFactory = CreateTabletFactory(tableType, tabletOptions);
    if (!tabletFactory) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "create tablet factory failed, table_name[%s] table_type[%s]",
                               tableName.c_str(), tableType.c_str());
    }
    auto schemaResolver = tabletFactory->CreateSchemaResolver();
    if (!schemaResolver) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "create schema resolver failed, table_name[%s] table_type[%s]",
                               tableName.c_str(), tableType.c_str());
    }
    auto status = TabletSchemaDelegation::Resolve(schemaResolver.get(), indexPath, schema);
    RETURN_IF_STATUS_ERROR(status, "schema resolve failed");
    return TabletSchemaDelegation::Check(schemaResolver.get(), *schema);
}

schemaid_t TabletSchemaLoader::GetSchemaId(const std::string& schemaFileName)
{
    schemaid_t id = DEFAULT_SCHEMAID;
    if (!ExtractSchemaId(schemaFileName, id)) {
        return std::numeric_limits<schemaid_t>::max();
    }
    return id;
}

bool TabletSchemaLoader::ExtractSchemaId(const std::string& fileName, schemaid_t& schemaId)
{
    const std::string prefix = SCHEMA_FILE_NAME;
    if (fileName.find(prefix) != 0) {
        return false;
    }
    if (fileName.length() == prefix.length()) {
        schemaId = DEFAULT_SCHEMAID;
        return true;
    }
    if (fileName[prefix.length()] != '.') {
        return false;
    }
    std::string idStr = fileName.substr(prefix.length() + 1);
    return autil::StringUtil::fromString(idStr, schemaId);
}

bool TabletSchemaLoader::IsNotSchemaFile(const std::string& fileName)
{
    schemaid_t id;
    return !ExtractSchemaId(fileName, id);
}

std::unique_ptr<ITabletFactory>
TabletSchemaLoader::CreateTabletFactory(const std::string& tableType,
                                        const std::shared_ptr<config::TabletOptions>& options)
{
    auto tabletFactoryCreator = TabletFactoryCreator::GetInstance();
    auto tabletFactory = tabletFactoryCreator->Create(tableType);
    if (!tabletFactory) {
        AUTIL_LOG(ERROR, "create tablet factory with type [%s] failed, registered types [%s]", tableType.c_str(),
                  autil::legacy::ToJsonString(tabletFactoryCreator->GetRegisteredType(), true).c_str());
        return nullptr;
    }
    if (!tabletFactory->Init(options, nullptr)) {
        AUTIL_LOG(ERROR, "init tablet factory with type [%s] failed", tableType.c_str());
        return nullptr;
    }
    return tabletFactory;
}

} // namespace indexlibv2::framework
