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
#include <mutex>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
namespace indexlib { namespace file_system {
class IFileSystem;
}} // namespace indexlib::file_system
namespace indexlibv2::config {
class TabletOptions;
class ITabletSchema;
} // namespace indexlibv2::config
namespace indexlibv2::framework {
class ITabletFactory;
class Version;

class TabletSchemaManager : private autil::NoCopyable
{
public:
    TabletSchemaManager(const std::shared_ptr<ITabletFactory>& tabletFactory,
                        const std::shared_ptr<config::TabletOptions>& tabletOptions, const std::string& globalRoot,
                        const std::string& remoteRoot,
                        const std::shared_ptr<indexlib::file_system::IFileSystem>& fileSystem);
    ~TabletSchemaManager();

public:
    std::map<schemaid_t, std::shared_ptr<config::ITabletSchema>> GetTabletSchemaCache() const;
    std::shared_ptr<config::ITabletSchema> GetSchema(schemaid_t schemaId) const;
    void InsertSchemaToCache(const std::shared_ptr<config::ITabletSchema>& schema);
    Status LoadAllSchema(const Version& version);
    Status StoreSchema(const config::ITabletSchema& schema);
    // get schemaList from baseSchemaId to lastSchemaId in versin roadmap
    Status GetSchemaList(schemaid_t baseSchema, schemaid_t targetVersion, const Version& version,
                         std::vector<std::shared_ptr<config::ITabletSchema>>& schemaList);

private:
    Status LoadSchema(schemaid_t schemaId);

private:
    mutable std::mutex _schemaCacheMutex;
    std::shared_ptr<ITabletFactory> _tabletFactory;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::string _globalRoot;
    std::string _remoteRoot;
    std::shared_ptr<indexlib::file_system::IFileSystem> _fileSystem;
    std::map<schemaid_t, std::shared_ptr<config::ITabletSchema>> _tabletSchemaCache;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
