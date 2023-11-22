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

#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/CommitOptions.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/ImportOptions.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/OpenOptions.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/framework/VersionMeta.h"

namespace indexlibv2::document {
class IDocumentBatch;
} // namespace indexlibv2::document

namespace indexlibv2::config {
class ITabletSchema;
class TabletOptions;
} // namespace indexlibv2::config

namespace indexlibv2::framework {
class TabletInfos;

class ITablet : private autil::NoCopyable
{
public:
    virtual ~ITablet() = default;

public:
    virtual Status Open(const IndexRoot& indexRoot, const std::shared_ptr<config::ITabletSchema>& schema,
                        const std::shared_ptr<config::TabletOptions>& options, const VersionCoord& versionCoord) = 0;
    virtual Status Reopen(const ReopenOptions& reopenOptions, const VersionCoord& versionCoord) = 0;
    virtual void Close() = 0;

public:
    // write
    virtual Status Build(const std::shared_ptr<document::IDocumentBatch>& batch) = 0;
    virtual Status Flush() = 0;
    virtual Status Seal() = 0;
    virtual bool NeedCommit() const = 0;
    virtual std::pair<Status, VersionMeta> Commit(const CommitOptions& commitOptions) = 0;
    virtual Status CleanIndexFiles(const std::vector<versionid_t>& reservedVersions) = 0;
    virtual Status CleanUnreferencedDeployFiles(const std::set<std::string>& toKeepFiles) = 0;
    virtual Status AlterTable(const std::shared_ptr<config::ITabletSchema>& newSchema) = 0;
    virtual std::pair<Status, versionid_t> ExecuteTask(const Version& sourceVersion, const std::string& taskType,
                                                       const std::string& taskName,
                                                       const std::map<std::string, std::string>& params) = 0;
    virtual Status Import(const std::vector<Version>& versions, const ImportOptions& options) = 0;
    virtual Status ImportExternalFiles(const std::string& bulkloadId, const std::vector<std::string>& externalFiles,
                                       const std::shared_ptr<ImportExternalFileOptions>& options, Action action,
                                       int64_t eventTimeInSecs) = 0;

public:
    // read
    virtual std::shared_ptr<ITabletReader> GetTabletReader() const = 0;
    virtual const TabletInfos* GetTabletInfos() const = 0;
    virtual std::shared_ptr<config::ITabletSchema> GetTabletSchema() const = 0;
    virtual std::shared_ptr<config::TabletOptions> GetTabletOptions() const = 0;
};

using TabletPtrMap = std::map<std::string, std::shared_ptr<ITablet>>;

} // namespace indexlibv2::framework
