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
#include "master/HoldMatrix.h"
#include "carbon/Log.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, HoldMatrix);

const static int32_t NaN = -1;

HoldMatrix::HoldMatrix(const map<roleid_t, PercentMap> &roleVerPercentMap,
                       const version_t &latestGroupVersion,
                       int32_t minHealthCapacity,
                       int32_t latestVersionRatio)
    : _minHealthCapacity(minHealthCapacity),
      _latestVersionRatio(latestVersionRatio)
{
    buildVersionIndexMap(roleVerPercentMap, latestGroupVersion);
    buildRoleIndexMap(roleVerPercentMap);
    initAvailMatrix(roleVerPercentMap);
    calcHoldPercent(roleVerPercentMap);
}

void HoldMatrix::buildVersionIndexMap(const map<roleid_t, PercentMap> &roleVerPercentMap,
                                      const version_t &latestGroupVersion)
{
    set<version_t> verSet;
    for (map<roleid_t, PercentMap>::const_iterator it = roleVerPercentMap.begin();
         it != roleVerPercentMap.end(); it++)
    {
        for (PercentMap::const_iterator pit = it->second.begin();
             pit != it->second.end(); pit++)
        {
            verSet.insert(pit->first);
        }
    }

    size_t verNums = verSet.size();
    _versionIndexMap[latestGroupVersion] = verNums - 1;
    _versionIndexReverseMap[verNums - 1] = latestGroupVersion;
    size_t versionIndex = 0;
    for (set<version_t>::iterator it = verSet.begin(); it != verSet.end(); it++) {
        if (*it == latestGroupVersion) {
            continue;
        }
        _versionIndexMap[*it] = versionIndex;
        _versionIndexReverseMap[versionIndex] = *it;
        versionIndex++;
    }
}

void HoldMatrix::buildRoleIndexMap(const map<roleid_t, PercentMap> &roleVerPercentMap) {
    size_t roleIndex = 0;
    for (map<roleid_t, PercentMap>::const_iterator it = roleVerPercentMap.begin();
         it != roleVerPercentMap.end(); it++)
    {
        _roleIndexMap[it->first] = roleIndex;
        _roleIndexReverseMap[roleIndex] = it->first;
        roleIndex++;
    }
}

void HoldMatrix::initAvailMatrix(const map<roleid_t, PercentMap> &roleVerPercentMap) {
    
    _availMatrix.resize(_roleIndexMap.size());
    for (size_t i = 0; i < _roleIndexMap.size(); i++) {
        _availMatrix[i].resize(_versionIndexMap.size(), NaN);
    }

    for (map<roleid_t, PercentMap>::const_iterator it = roleVerPercentMap.begin();
         it != roleVerPercentMap.end(); it++)
    {
        const roleid_t &roleId = it->first;
        size_t roleIndex = _roleIndexMap[roleId];
        
        for (PercentMap::const_iterator pit = it->second.begin();
             pit != it->second.end(); pit++)
        {
            size_t versionIndex = _versionIndexMap[pit->first];
            _availMatrix[roleIndex][versionIndex] = pit->second;
        }
    }
    genAvailMatrixStr();
}    

void HoldMatrix::genAvailMatrixStr() {
    _availMatrixStr = "\n";
    for (size_t i = 0; i < _versionIndexMap.size(); i++) {
        _availMatrixStr += _versionIndexReverseMap[i] + ",";
    }
    _availMatrixStr += "\n";
    for (size_t i = 0; i < _availMatrix.size(); i++) {
        _availMatrixStr += _roleIndexReverseMap[i] + ",";
        for (size_t j = 0; j < _availMatrix[i].size(); j++) {
            _availMatrixStr += to_string(_availMatrix[i][j]) + ",";
        }
        _availMatrixStr += "\n";
    }
}

void HoldMatrix::calcHoldPercent(const map<roleid_t, PercentMap> &roleVerPercentMap) {
    vector<vector<int32_t> > holdMatrix;
    holdMatrix.resize(_roleIndexMap.size());
    for (size_t i = 0; i < holdMatrix.size(); i++) {
        holdMatrix[i].resize(_versionIndexMap.size());
    }

    calcHoldMatrix(&holdMatrix);

    transferMatrix(holdMatrix, roleVerPercentMap);
}

void HoldMatrix::holdVersion(vector<vector<int32_t> > *holdMatrix,
                             int32_t verIndex,
                             int32_t holdUpperBound,
                             bool setNaN)
{
    int32_t minAvailPercent = calcAvailCount(verIndex);
    minAvailPercent = min(holdUpperBound, minAvailPercent);
    if (_minHealthCapacity > minAvailPercent) {
        _minHealthCapacity -= minAvailPercent;
    } else {
        minAvailPercent = _minHealthCapacity;
        _minHealthCapacity = 0;
    }
    bool alreadyFindMin = false;
    for (size_t i = 0; i < _roleIndexMap.size(); i++) {
        if (_availMatrix[i][verIndex] != NaN) {
            _availMatrix[i][verIndex] -= minAvailPercent;
            (*holdMatrix)[i][verIndex] += minAvailPercent;
        }
        if (!setNaN) {
            continue;
        }
        if (_availMatrix[i][verIndex] == 0 && !alreadyFindMin) {
            _availMatrix[i][verIndex] = NaN;
            alreadyFindMin = true;
        }
    }
}

void HoldMatrix::calcHoldMatrix(vector<vector<int32_t> > *holdMatrix) {
    // holdFullLine by latestRatio
    holdVersion(holdMatrix, _versionIndexMap.size() - 1, _latestVersionRatio, false);
    for (int32_t verIndex = _versionIndexMap.size() - 2; verIndex >= 0; verIndex--)
    {
        holdVersion(holdMatrix, verIndex, _minHealthCapacity, false);
    }
    size_t calcRound = 0;
    while (calcRound++ < _roleIndexMap.size()) {
        for (int32_t verIndex = _versionIndexMap.size() - 1; verIndex >= 0; verIndex--) {
            if (_minHealthCapacity == 0) {
                return;
            }
            holdVersion(holdMatrix, verIndex, _minHealthCapacity, true);
        }
    }
}

int32_t HoldMatrix::calcAvailCount(int32_t verIndex) {
    int32_t availCount = std::numeric_limits<int32_t>::max();
    for (size_t i = 0; i < _roleIndexMap.size(); i++) {
        if (_availMatrix[i][verIndex] != NaN) {
            availCount = min(availCount, _availMatrix[i][verIndex]);
        }
    }
    if (availCount == std::numeric_limits<int32_t>::max()) {
        return 0;
    }
    return availCount;
}

void HoldMatrix::transferMatrix(
        const vector<vector<int32_t> > &holdMatrix,
        const map<roleid_t, PercentMap> &roleVerPercentMap)
{
    for (map<roleid_t, PercentMap>::const_iterator it = roleVerPercentMap.begin();
         it != roleVerPercentMap.end(); it++)
    {
        for (PercentMap::const_iterator pit = it->second.begin();
             pit != it->second.end(); pit++)
        {
            size_t verIndex = _versionIndexMap[pit->first];
            size_t roleIndex = _roleIndexMap[it->first];
            _roleVerHoldPercentMap[it->first][pit->first] =
                holdMatrix[roleIndex][verIndex];
        }
    }
}

END_CARBON_NAMESPACE(master);

