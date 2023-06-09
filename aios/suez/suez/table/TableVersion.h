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

#include "autil/legacy/jsonizable.h"
#include "indexlib/framework/VersionMeta.h"
#include "suez/sdk/PartitionId.h"

namespace suez {

class TableVersion : public autil::legacy::Jsonizable {
public:
    TableVersion();
    // generator should be ip:port
    TableVersion(IncVersion versionId,
                 const indexlibv2::framework::VersionMeta &versionMeta = indexlibv2::framework::VersionMeta(),
                 bool finished = false);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;

public:
    TableVersion &setBranchId(uint64_t branchId);
    uint64_t getBranchId() const;

    TableVersion &setVersionId(IncVersion version);
    IncVersion getVersionId() const;

    TableVersion &setVersionMeta(const indexlibv2::framework::VersionMeta &meta);
    const indexlibv2::framework::VersionMeta &getVersionMeta() const;

    TableVersion &setGenerator(const std::string &generator);
    const std::string &getGenerator() const;

    TableVersion &setFinished(bool flag);
    bool isFinished() const;

    TableVersion &setIsLeaderVersion(bool flag);
    bool isLeaderVersion() const;

    std::string toString() const;

public:
    bool operator==(const TableVersion &other) const;
    bool operator!=(const TableVersion &other) const;
    bool operator<(const TableVersion &other) const;

private:
    uint64_t _branchId;
    IncVersion _versionId;
    indexlibv2::framework::VersionMeta _versionMeta;
    bool _finished;
    bool _leaderVersion;
    std::string _generator;
};

} // namespace suez
