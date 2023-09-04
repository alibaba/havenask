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
#include "build_service/admin/ProhibitedIpCollector.h"

#include "autil/TimeUtility.h"
#include "build_service/admin/ProhibitedIpsTable.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ProhibitedIpCollector);

ProhibitedIpCollector::ProhibitedIpCollector(ProhibitedIpsTable* table) : _table(table)
{
    _voteInterval = ProhibitedIpsTable::VOTE_INTERVAL;
}

ProhibitedIpCollector::~ProhibitedIpCollector() {}

void ProhibitedIpCollector::collect(const std::string& ip)
{
    int64_t currentTime = TimeUtility::currentTimeInMicroSeconds();
    auto iter = _ips.find(ip);
    if (iter != _ips.end()) {
        if (currentTime - iter->second < _voteInterval) {
            return;
        }
    }

    _ips[ip] = currentTime;
    _table->addProhibitIp(ip);
    std::map<std::string, int64_t> ips;
    for (auto iter = _ips.begin(); iter != _ips.end(); iter++) {
        if (currentTime - iter->second < _voteInterval) {
            ips[iter->first] = iter->second;
        }
    }
    _ips.swap(ips);
}

}} // namespace build_service::admin
