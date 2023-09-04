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
#include "indexlib/framework/TabletSchemaManager.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/framework/ITabletFactory.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/Version.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, TabletSchemaManager);

TabletSchemaManager::TabletSchemaManager(const std::shared_ptr<ITabletFactory>& tabletFactory,
                                         const std::shared_ptr<config::TabletOptions>& tabletOptions,
                                         const std::string& globalRoot, const std::string& remoteRoot,
                                         const std::shared_ptr<indexlib::file_system::IFileSystem>& fileSystem)
    : _tabletFactory(tabletFactory)
    , _tabletOptions(tabletOptions)
    , _globalRoot(globalRoot)
    , _remoteRoot(remoteRoot)
    , _fileSystem(fileSystem)
{
}

TabletSchemaManager::~TabletSchemaManager() {}

std::shared_ptr<config::ITabletSchema> TabletSchemaManager::GetSchema(schemaid_t schemaId) const
{
    std::lock_guard<std::mutex> guard(_schemaCacheMutex);
    auto iter = _tabletSchemaCache.find(schemaId);
    if (iter != _tabletSchemaCache.end()) {
        return iter->second;
    }
    return nullptr;
}

void TabletSchemaManager::InsertSchemaToCache(const std::shared_ptr<config::ITabletSchema>& schema)
{
    std::lock_guard<std::mutex> guard(_schemaCacheMutex);
    _tabletSchemaCache[schema->GetSchemaId()] = schema;
}

Status TabletSchemaManager::GetSchemaList(schemaid_t baseSchema, schemaid_t targetSchema, const Version& version,
                                          std::vector<std::shared_ptr<config::ITabletSchema>>& schemaList)
{
    const auto& schemaIdList = version.GetSchemaVersionRoadMap();
    for (auto schemaId : schemaIdList) {
        if (schemaId < baseSchema || schemaId > targetSchema) {
            continue;
        }
        auto schema = GetSchema(schemaId);
        if (!schema) {
            AUTIL_LOG(ERROR, "get schema failed [%d]", schemaId);
            return Status::Corruption();
        }
        schemaList.push_back(schema);
    }
    assert(GetSchema(baseSchema));
    assert(GetSchema(targetSchema));
    return Status::OK();
}

Status TabletSchemaManager::StoreSchema(const config::ITabletSchema& schema)
{
    auto rootDirectory = indexlib::file_system::IDirectory::GetPhysicalDirectory(_globalRoot);
    std::string schemaFileName = schema.GetSchemaFileName();
    std::string content;
    if (!schema.Serialize(/*isCompact*/ false, &content)) {
        AUTIL_LOG(ERROR, "serialize schema failed");
        return Status::Corruption();
    }
    auto status = rootDirectory->Store(schemaFileName, content, indexlib::file_system::WriterOption()).Status();
    if (!status.IsOK()) {
        if (status.IsExist()) {
            AUTIL_LOG(WARN, "schema [%s] already exist", schemaFileName.c_str());
        } else {
            AUTIL_LOG(ERROR, "store schema [%s] failed: %s", schemaFileName.c_str(), status.ToString().c_str());
            return status;
        }
    }
    status = _fileSystem
                 ->MountFile(rootDirectory->GetRootDir(), schemaFileName, schemaFileName,
                             indexlib::file_system::FSMT_READ_ONLY, content.size(), /*mayNonExist=*/false)
                 .Status();
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "mount schema [%s] failed: %s", schemaFileName.c_str(), status.ToString().c_str());
        return status;
    }
    return Status::OK();
}

Status TabletSchemaManager::LoadAllSchema(const Version& version)
{
    const auto& schemaList = version.GetSchemaVersionRoadMap();
    for (auto schemaId : schemaList) {
        auto schema = GetSchema(schemaId);
        if (schema) {
            continue;
        }
        auto status = LoadSchema(schemaId);
        RETURN_IF_STATUS_ERROR(status, "load schema file [%d] failed", schemaId);
    }
    return Status::OK();
}

Status TabletSchemaManager::LoadSchema(schemaid_t schemaId)
{
    auto rootDirectory = indexlib::file_system::IDirectory::Get(_fileSystem);
    std::shared_ptr<config::TabletSchema> schema = TabletSchemaLoader::GetSchema(rootDirectory, schemaId);
    if (!schema) {
        RETURN_IF_STATUS_ERROR(Status::InternalError(), "load schema [%d] failed", schemaId);
    }
    auto status = TabletSchemaLoader::ResolveSchema(_tabletOptions, _remoteRoot, schema.get());
    if (status.IsOK()) {
        InsertSchemaToCache(schema);
    }
    return status;
}

std::map<schemaid_t, std::shared_ptr<config::ITabletSchema>> TabletSchemaManager::GetTabletSchemaCache() const
{
    std::lock_guard<std::mutex> guard(_schemaCacheMutex);
    return _tabletSchemaCache;
}

} // namespace indexlibv2::framework
