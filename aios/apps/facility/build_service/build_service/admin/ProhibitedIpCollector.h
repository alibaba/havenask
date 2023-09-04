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
#ifndef ISEARCH_BS_PROHIBITEDIPCOLLECTOR_H
#define ISEARCH_BS_PROHIBITEDIPCOLLECTOR_H

#include <unordered_set>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class ProhibitedIpsTable;
class ProhibitedIpCollector
{
public:
    ProhibitedIpCollector(ProhibitedIpsTable* table);
    ~ProhibitedIpCollector();

private:
    ProhibitedIpCollector(const ProhibitedIpCollector&);
    ProhibitedIpCollector& operator=(const ProhibitedIpCollector&);

public:
    void collect(const std::string& ip);

private:
    ProhibitedIpsTable* _table;
    std::map<std::string, int64_t> _ips;
    int64_t _voteInterval;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProhibitedIpCollector);

}} // namespace build_service::admin

#endif // ISEARCH_BS_PROHIBITEDIPCOLLECTOR_H
