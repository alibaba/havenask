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
#include "build_service/config/RealtimeSchemaListKeeper.h"

#include "build_service/util/IndexPathConstructor.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/TabletSchemaLoader.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, RealtimeSchemaListKeeper);

const std::string RealtimeSchemaListKeeper::REALTIME_SCHEMA_LIST = "schema_list";
RealtimeSchemaListKeeper::RealtimeSchemaListKeeper() {}

RealtimeSchemaListKeeper::~RealtimeSchemaListKeeper() {}
void RealtimeSchemaListKeeper::init(const std::string& indexRoot, const std::string& clusterName, uint32_t generationId)
{
    _clusterName = clusterName;
    _schemaListDir = indexlib::file_system::FslibWrapper::JoinPath(
        util::IndexPathConstructor::getGenerationIndexPath(indexRoot, clusterName, generationId), REALTIME_SCHEMA_LIST);
}

bool RealtimeSchemaListKeeper::addSchema(const std::shared_ptr<indexlibv2::config::TabletSchema>& schema)
{
    if (_schemaCache.empty()) {
        auto status = indexlib::file_system::FslibWrapper::MkDirIfNotExist(_schemaListDir).Status();
        if (!status.IsOK()) {
            BS_LOG(ERROR, "mkdir schema list dir [%s] failed, [%s]", _schemaListDir.c_str(), status.ToString().c_str());
            return false;
        }
    }
    auto iter = _schemaCache.find(schema->GetSchemaId());
    if (iter != _schemaCache.end()) {
        return true;
    }
    auto schemaFileName = schema->GetSchemaFileName();
    auto schemaPath = indexlib::file_system::FslibWrapper::JoinPath(_schemaListDir, schemaFileName);
    std::string schemaStr;
    if (!schema->Serialize(false, &schemaStr)) {
        return false;
    }
    if (!indexlib::file_system::FslibWrapper::AtomicStore(schemaPath, schemaStr, true).OK()) {
        BS_LOG(ERROR, "store schema failed");
        return false;
    }
    _schemaCache.insert(std::make_pair(schema->GetSchemaId(), schema));
    return true;
}

bool RealtimeSchemaListKeeper::getSchemaList(indexlib::schemaid_t beginSchemaId, indexlib::schemaid_t endSchemaId,
                                             std::vector<std::shared_ptr<indexlibv2::config::TabletSchema>>& schemaList)
{
    schemaList.clear();
    for (auto [schemaId, tabletSchema] : _schemaCache) {
        if (schemaId >= beginSchemaId && schemaId <= endSchemaId) {
            schemaList.push_back(tabletSchema);
        }
    }
    return true;
}

std::shared_ptr<indexlibv2::config::TabletSchema>
RealtimeSchemaListKeeper::getTabletSchema(indexlib::schemaid_t targetSchemaId)
{
    for (auto [schemaId, tabletSchema] : _schemaCache) {
        if (schemaId == targetSchemaId) {
            return tabletSchema;
        }
    }
    return nullptr;
}

bool RealtimeSchemaListKeeper::updateSchemaCache()
{
    auto directory = indexlib::file_system::IDirectory::GetPhysicalDirectory(_schemaListDir);
    fslib::FileList fileList;
    auto status = indexlibv2::framework::TabletSchemaLoader::ListSchema(directory, &fileList);
    if (status.IsNotFound()) {
        return true;
    }
    if (!status.IsOK()) {
        return false;
    }
    for (auto fileName : fileList) {
        auto schemaId = indexlibv2::framework::TabletSchemaLoader::GetSchemaId(fileName);
        if (_schemaCache.find(schemaId) != _schemaCache.end()) {
            continue;
        }
        auto tabletSchema = indexlibv2::framework::TabletSchemaLoader::GetSchema(directory, schemaId);
        if (!tabletSchema) {
            return false;
        }
        _schemaCache.insert(
            std::make_pair(schemaId, std::shared_ptr<indexlibv2::config::TabletSchema>(tabletSchema.release())));
    }
    return true;
}

}} // namespace build_service::config
