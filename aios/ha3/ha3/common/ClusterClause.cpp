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
#include "ha3/common/ClusterClause.h"

#include <algorithm>
#include <cstddef>

#include "autil/DataBuffer.h"

#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, ClusterClause);

#define CLUSTER_CLAUSE_EXTEND_SERIALIZE_SECTION_NAME "HA3_CLUSTER_CLAUSE_EXTEND"

ClusterClause::ClusterClause() {
    _totalSearchPartCnt = 0;
}

ClusterClause::~ClusterClause() {
}

const ClusterPartIdsMap& ClusterClause::getClusterPartIdsMap() const {
    return _clusterPartIdsMap;
}

const ClusterPartIdPairsMap& ClusterClause::getClusterPartIdPairsMap() const {
    return _clusterPartIdPairsMap;
}

bool ClusterClause::emptyPartIds() const {
    if (_clusterPartIdPairsMap.empty()) {
        bool empty = true;  // use in doc limit and compatiblity with old qrs
        ClusterPartIdsMap::const_iterator it = _clusterPartIdsMap.begin();
        for (; it != _clusterPartIdsMap.end(); it++) {
            if (!it->second.empty()) {
                empty = false;
                break;
            }
        }
        return empty;
    }
    bool empty = true;
    ClusterPartIdPairsMap::const_iterator it = _clusterPartIdPairsMap.begin();
    for (; it != _clusterPartIdPairsMap.end(); it++) {
        if (!it->second.empty()) {
            empty = false;
            break;
        }
    }
    return empty;
}


void ClusterClause::addClusterPartIds(const string &clusterName,
                                      const vector<hashid_t> &partids)
{
    const string& zoneName = cluster2ZoneName(clusterName);
    vector<string>::const_iterator it = _clusterNameVec.begin();
    for (; it != _clusterNameVec.end(); ++it) {
        if (*it == zoneName) {
            break;
        }
    }
    if (it == _clusterNameVec.end()) {
        _clusterNameVec.push_back(zoneName);
    }
    _clusterPartIdsMap[zoneName] = partids; // compatible with plugin

    vector<pair<hashid_t, hashid_t> > tmpPartids;
    for (auto hashid : partids) {
        tmpPartids.emplace_back(hashid, hashid);
    }
    _clusterPartIdPairsMap[zoneName] = tmpPartids;
}

void ClusterClause::addClusterPartIds(const string &clusterName,
                                      const vector<pair<hashid_t, hashid_t> > &partids)
{
    const string& zoneName = cluster2ZoneName(clusterName);
    vector<string>::const_iterator it = _clusterNameVec.begin();
    for (; it != _clusterNameVec.end(); ++it) {
        if (*it == zoneName) {
            break;
        }
    }
    if (it == _clusterNameVec.end()) {
        _clusterNameVec.push_back(zoneName);
    }
    _clusterPartIdPairsMap[zoneName] = partids;
    // compatible with to old searcher, only for one-way routing hash
    vector<hashid_t> idVec;
    for (auto idPair : partids) {
        if (idPair.first == idPair.second) {
            idVec.push_back(idPair.first);
        }
    }
    _clusterPartIdsMap[zoneName] = idVec;
}

bool ClusterClause::getClusterPartIds(const string &clusterName,
                                      vector<pair<hashid_t, hashid_t> > &partids) const
{
    if (_clusterPartIdPairsMap.empty()) {  // from old qrs
        partids.clear();
        ClusterPartIdsMap::const_iterator it = _clusterPartIdsMap.find(clusterName);
        if (it != _clusterPartIdsMap.end()) {
            for (auto hashid : it->second) {
                partids.emplace_back(hashid, hashid);
            }
            return true;
        }
    }
    ClusterPartIdPairsMap::const_iterator it = _clusterPartIdPairsMap.find(clusterName);
    if (it != _clusterPartIdPairsMap.end()) {
        partids = it->second;
        return true;
    }
    return false;
}

void ClusterClause::clearClusterName() {
    _clusterNameVec.clear();
    _clusterPartIdsMap.clear();
    _clusterPartIdPairsMap.clear();
    _totalSearchPartCnt = 0;
}


size_t ClusterClause::getClusterNameCount() const {
    return _clusterNameVec.size();
}

string ClusterClause::getClusterName(size_t index) const {
    if (getClusterNameCount() == 0 || index >= getClusterNameCount()) {
        return "";
    }
    return _clusterNameVec[index];
}

void ClusterClause::addClusterName(const string &clusterName) {
    string zoneName = cluster2ZoneName(clusterName);
    if (find(_clusterNameVec.begin(), _clusterNameVec.end(), zoneName)
        == _clusterNameVec.end())
    {
        _clusterNameVec.push_back(zoneName);
    }
}


const vector<string>& ClusterClause::getClusterNameVector() const {
    return _clusterNameVec;
}

void ClusterClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_clusterPartIdsMap);
    dataBuffer.write(_clusterNameVec);
    dataBuffer.write(_originalString);
    serializeExtFields(dataBuffer);
}

void ClusterClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_clusterPartIdsMap);
    dataBuffer.read(_clusterNameVec);
    dataBuffer.read(_originalString);
    deserializeExtFields(dataBuffer);
}

void ClusterClause::serializeExtFields(autil::DataBuffer &dataBuffer) const {
    autil::DataBuffer *extFieldsBuffer =
        dataBuffer.declareSectionBuffer(CLUSTER_CLAUSE_EXTEND_SERIALIZE_SECTION_NAME);
    if (extFieldsBuffer) {
        extFieldsBuffer->write(_clusterPartIdPairsMap);
        extFieldsBuffer->write(_totalSearchPartCnt);
    }
}

void ClusterClause::deserializeExtFields(autil::DataBuffer &dataBuffer) {
    autil::DataBuffer *extFieldsBuffer =
        dataBuffer.findSectionBuffer(CLUSTER_CLAUSE_EXTEND_SERIALIZE_SECTION_NAME);
    if (extFieldsBuffer) {
        extFieldsBuffer->read(_clusterPartIdPairsMap);
        extFieldsBuffer->read(_totalSearchPartCnt);
    }
}
void ClusterClause::setTotalSearchPartCnt(uint32_t partCnt) {
    _totalSearchPartCnt = partCnt;
}
uint32_t ClusterClause::getToalSearchPartCnt() const {
    return _totalSearchPartCnt;
}



} // namespace common
} // namespace isearch
