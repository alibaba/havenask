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
#ifndef CARBON_HOLDMATRIX_H
#define CARBON_HOLDMATRIX_H

#include "common/common.h"
#include "carbon/Log.h"
#include "master/Role.h"
BEGIN_CARBON_NAMESPACE(master);

class HoldMatrix
{
public:
    HoldMatrix(const std::map<roleid_t, PercentMap> &roleVerPercentMap,
               const version_t &latestGroupVersion, int32_t minHealthCapacity,
               int32_t latestVersionRatio = 100);
    ~HoldMatrix() {}

public:
    const std::map<roleid_t, PercentMap>& getHoldPercent() {
        return _roleVerHoldPercentMap;
    }

    const std::string& getAvailMatrixStr() const {
        return _availMatrixStr;
    }
private:
    HoldMatrix(const HoldMatrix &);
    HoldMatrix& operator=(const HoldMatrix &);

    void buildVersionIndexMap(const std::map<roleid_t, PercentMap> &roleVerPercentMap,
                              const version_t &latestGroupVersion);

    void buildRoleIndexMap(const std::map<roleid_t, PercentMap> &roleVerPercentMap);

    void initAvailMatrix(const std::map<roleid_t, PercentMap> &roleVerPercentMap);

    void calcHoldPercent(const std::map<roleid_t, PercentMap> &roleVerPercentMap);

    void calcHoldMatrix(std::vector<std::vector<int32_t> > *holdMatrix);

    void holdVersion(std::vector<std::vector<int32_t> > *holdMatrix,
                     int32_t verIndex,
                     int32_t holdUpperBound,
                     bool setNaN);

    void transferMatrix(
            const std::vector<std::vector<int32_t> > &holdMatrix,
            const std::map<roleid_t, PercentMap> &roleVerPercentMap);

    int32_t calcAvailCount(int32_t verIndex);

    void genAvailMatrixStr();
private:
    int32_t _minHealthCapacity;
    int32_t _latestVersionRatio;
    std::string _availMatrixStr;
    std::vector<std::vector<int32_t> > _availMatrix;
    std::map<version_t, size_t> _versionIndexMap;
    std::map<size_t, version_t> _versionIndexReverseMap;
    std::map<roleid_t, size_t> _roleIndexMap;
    std::map<size_t, roleid_t> _roleIndexReverseMap;
    std::map<roleid_t, PercentMap> _roleVerHoldPercentMap;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(HoldMatrix);

END_CARBON_NAMESPACE(master);

#endif //CARBON_HOLDMATRIX_H
