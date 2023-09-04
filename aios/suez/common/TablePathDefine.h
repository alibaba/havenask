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

#include <string>

#include "suez/sdk/PartitionId.h"

namespace suez {

class TablePathDefine {
private:
    TablePathDefine();
    ~TablePathDefine();
    TablePathDefine(const TablePathDefine &);
    TablePathDefine &operator=(const TablePathDefine &);

public:
    static std::string getGenerationDir(const PartitionId &pid);
    static std::string getIndexGenerationPrefix(const PartitionId &pid);
    static std::string getIndexPartitionPrefix(const PartitionId &pid);
    static std::string getGenerationMetaFilePath(const PartitionId &pid);
    static std::string getRealtimeInfoFilePath(const PartitionId &pid);
    static std::string constructIndexPath(const std::string &indexRoot, const PartitionId &pid);
    static std::string constructIndexPath(const std::string &indexRoot,
                                          const std::string &tableName,
                                          uint32_t generationId,
                                          const std::string &rangeName);

    static std::string constructLocalDataPath(const PartitionId &pid, const std::string &dirName);
    static std::string constructLocalDataPath(const std::string &name, int64_t version, const std::string &dirName);
    static std::string constructLocalConfigPath(const std::string &tableName, const std::string &remoteConfigPath);
    static std::string constructTempConfigPath(const std::string &tablename,
                                               uint32_t from,
                                               uint32_t to,
                                               const std::string &remoteConfigPath);
    static std::string constructPartitionVersionFileName(const PartitionId &pid);
    static std::string constructPartitionLeaderElectPath(const PartitionId &pid, bool ignoreVersion);
    static std::string
    constructVersionPublishFilePath(const std::string &indexRoot, const PartitionId &pid, IncVersion versionId);
    static std::string getVersionPublishFileName(IncVersion versionId);
    static std::string constructPartitionDir(const PartitionId &pid);

public:
    static const std::string GENERATION_PREFIX;
    static const std::string PARTITION_PREFIX;
    static const std::string GENERATION_META_FILE;
};

} // namespace suez
