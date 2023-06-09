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
#include "suez/table/TableVersion.h"

#include "suez/sdk/IpUtil.h"

namespace suez {

TableVersion::TableVersion()
    : _branchId(0), _versionId(indexlibv2::INVALID_VERSIONID), _finished(false), _leaderVersion(false) {}

TableVersion::TableVersion(IncVersion versionId, const indexlibv2::framework::VersionMeta &meta, bool finished)
    : _branchId(0), _versionId(versionId), _versionMeta(meta), _finished(finished), _leaderVersion(false) {
    _generator = IpUtil::getHostAddress();
}

void TableVersion::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("branch_id", _branchId, _branchId);
    json.Jsonize("version_id", _versionId, _versionId);
    json.Jsonize("version_meta", _versionMeta, _versionMeta);
    json.Jsonize("finished", _finished, _finished);
    json.Jsonize("generator", _generator, _generator);
    json.Jsonize("is_leader", _leaderVersion, _leaderVersion);
}

TableVersion &TableVersion::setBranchId(uint64_t branchId) {
    _branchId = branchId;
    return *this;
}

uint64_t TableVersion::getBranchId() const { return _branchId; }

TableVersion &TableVersion::setVersionId(IncVersion version) {
    _versionId = version;
    return *this;
}

IncVersion TableVersion::getVersionId() const { return _versionId; }

TableVersion &TableVersion::setVersionMeta(const indexlibv2::framework::VersionMeta &meta) {
    _versionMeta = meta;
    return *this;
}

const indexlibv2::framework::VersionMeta &TableVersion::getVersionMeta() const { return _versionMeta; }

TableVersion &TableVersion::setGenerator(const std::string &generator) {
    _generator = generator;
    return *this;
}

const std::string &TableVersion::getGenerator() const { return _generator; }

TableVersion &TableVersion::setFinished(bool flag) {
    _finished = flag;
    return *this;
}

bool TableVersion::isFinished() const { return _finished; }

TableVersion &TableVersion::setIsLeaderVersion(bool flag) {
    _leaderVersion = flag;
    return *this;
}

bool TableVersion::isLeaderVersion() const { return _leaderVersion; }

bool TableVersion::operator==(const TableVersion &other) const {
    if (this == &other) {
        return true;
    }
    return _branchId == other._branchId && _versionId == other._versionId && _versionMeta == other._versionMeta &&
           _generator == other._generator && _finished == other._finished;
}

bool TableVersion::operator!=(const TableVersion &other) const { return !(*this == other); }

bool TableVersion::operator<(const TableVersion &other) const { return _versionId < other._versionId; }

std::string TableVersion::toString() const { return autil::legacy::ToJsonString(*this, true); }

} // namespace suez
