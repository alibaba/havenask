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
#include "build_service/admin/ProhibitedIpsTable.h"

#include "autil/TimeUtility.h"
using namespace std;
using namespace autil;
namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProhibitedIpsTable);

ProhibitedIpsTable::ProhibitedIpsTable(const int64_t& prohibitIpTime, const size_t& maxProhibitIpNum)
    : _timeToKeep(prohibitIpTime)
    , _maxProhibitIpNum(maxProhibitIpNum)
{
}

ProhibitedIpsTable::ProhibitedIpsTable()
{
    _timeToKeep = TWO_HOURS;
    _maxProhibitIpNum = DEFAULT_MAX_PROHIBITED_IP_NUM;
}

ProhibitedIpsTable::~ProhibitedIpsTable() {}

void ProhibitedIpsTable::addProhibitIp(const std::string& ip)
{
    autil::ScopedLock lock(_lock);
    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    ProhibitedIpMap::iterator iter = _prohibitedIpsMap.find(ip);
    if (iter == _prohibitedIpsMap.end()) {
        _prohibitedIpsMap[ip].reportTime = currentTime;
        _prohibitedIpsMap[ip].count = 1;
        return;
    }
    iter->second.reportTime = currentTime;
    iter->second.count++;
}

void ProhibitedIpsTable::collectCandidateIps(vector<IpItem>& candidateIps, int64_t currentTime)
{
    autil::ScopedLock lock(_lock);
    ProhibitedIpMap::iterator it = _prohibitedIpsMap.begin();
    for (; it != _prohibitedIpsMap.end();) {
        if (isProhibitedIp(it->second, currentTime)) {
            candidateIps.push_back(IpItem(it->first, it->second));
            it++;
            continue;
        }
        if (canFreeIp(it->second, currentTime)) {
            _prohibitedIpsMap.erase(it++);
            continue;
        }
        it++;
    }
}

bool ProhibitedIpsTable::isProhibitedIp(const IpDescription& description, int64_t currentTime)
{
    switch (description.count) {
    case 1:
        return currentTime - description.reportTime < FIFTEEN_MINUTES;
    case 2:
        return currentTime - description.reportTime < THIRTY_MINUTES;
    case 3:
        return currentTime - description.reportTime < ONE_HOUR;
    default:
        return currentTime - description.reportTime < _timeToKeep;
    }
}

bool ProhibitedIpsTable::canFreeIp(const IpDescription& description, int64_t currentTime)
{
    if (currentTime - description.reportTime > _timeToKeep) {
        return true;
    }
    return false;
}

void ProhibitedIpsTable::getProhibitedIps(std::vector<std::string>& prohibitedIps)
{
    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    vector<IpItem> candidateIps;
    collectCandidateIps(candidateIps, currentTime);

    std::sort(candidateIps.begin(), candidateIps.end(), [](const IpItem& a, const IpItem& b) -> bool {
        if (a.description.count > b.description.count) {
            return true;
        }
        if (a.description.count < b.description.count) {
            return false;
        }
        return a.description.reportTime >= b.description.reportTime;
    });
    for (auto it : candidateIps) {
        if (prohibitedIps.size() >= _maxProhibitIpNum) {
            break;
        }
        prohibitedIps.push_back(it.ip);
    }
}

}} // namespace build_service::admin
