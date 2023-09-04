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
#ifndef ISEARCH_BS_PROHIBITEDIPSTABLE_H
#define ISEARCH_BS_PROHIBITEDIPSTABLE_H

#include <unordered_map>

#include "build_service/admin/ProhibitedIpCollector.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "master_framework/ScheduleUnit.h"
namespace build_service { namespace admin {

class ProhibitedIpsTable
{
private:
    const static int64_t TWO_HOURS = 2 * 3600LL * 1000 * 1000;
    const static int64_t ONE_HOUR = 1 * 3600LL * 1000 * 1000;
    const static int64_t THIRTY_MINUTES = 1800LL * 1000 * 1000;
    const static int64_t FIFTEEN_MINUTES = 15LL * 60 * 1000 * 1000;
    const static size_t DEFAULT_MAX_PROHIBITED_IP_NUM = 10;

public:
    const static int64_t VOTE_INTERVAL = 15LL * 60 * 1000 * 1000;

private:
    struct IpDescription {
        int64_t reportTime;
        int64_t count;
    };

    struct IpItem {
        IpItem() {}
        IpItem(const std::string& _ip, const IpDescription& _description) : ip(_ip), description(_description) {}
        std::string ip;
        IpDescription description;
    };

    typedef std::unordered_map<std::string, IpDescription> ProhibitedIpMap;

public:
    ProhibitedIpsTable(const int64_t& timeStamp, const size_t& prohibitSize);
    ProhibitedIpsTable();
    ~ProhibitedIpsTable();

private:
    ProhibitedIpsTable(const ProhibitedIpsTable&);
    ProhibitedIpsTable& operator=(const ProhibitedIpsTable&);

public:
    ProhibitedIpCollectorPtr createCollector() { return ProhibitedIpCollectorPtr(new ProhibitedIpCollector(this)); }
    void addProhibitIp(const std::string& ip);
    void getProhibitedIps(std::vector<std::string>& prohibitedIps);

private:
    void collectCandidateIps(std::vector<IpItem>& candidateIps, int64_t currentTime);
    bool isProhibitedIp(const IpDescription& description, int64_t currentTime);
    bool canFreeIp(const IpDescription& description, int64_t currentTime);

private:
    ProhibitedIpMap _prohibitedIpsMap;
    mutable autil::ThreadMutex _lock;
    int64_t _timeToKeep;
    size_t _maxProhibitIpNum;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProhibitedIpsTable);

}} // namespace build_service::admin

#endif // ISEARCH_BS_PROHIBITEDIPSTABLE_H
