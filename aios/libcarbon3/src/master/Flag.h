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
#ifndef CARBON_MASTER_FLAG_H
#define CARBON_MASTER_FLAG_H
 
#include "common/common.h"
#include "common/EnvParser.h"
#include "carbon/Log.h"
#include "carbon/CommonDefine.h"

BEGIN_CARBON_NAMESPACE(master);

struct SchConf : public common::KVObject {
    SchConf() : schType(-1), routeAll(false) {}

    virtual bool fromString(const std::map<std::string, std::string>& kvs) {
        FROM_STRING(kvs, "proxy", host);
        FROM_STRING(kvs, "schType", schType);
        FROM_STRING(kvs, "routeAll", routeAll);
        FROM_STRING(kvs, "routeRegex", routeRegex);
        return true;
    }

    std::string host; // optional
    int schType; // see DriverOptions
    bool routeAll; // if route all groups to remote proxy. 
    std::string routeRegex; // optional, the group regex to route
};

#define K_CARBON_SCH_CONF "CARBON_SCH_CONF"
// TODO: refactor these fucking dirty names below.
#define K_LONG_CONNECTION "LONG_CONNECTION" // long connection to proxy
#define K_KMON_CLUSTER "CLUSTER" // kmon metric `cluster`

// Don't append more flags in this class, use EnvParser to get options instead.
class Flag 
{
public:
    static SchConf getSchConf() {
        return common::EnvParser::get<SchConf>(K_CARBON_SCH_CONF);
    }

    static std::string getCluster() {
        return common::EnvParser::get<std::string>(K_KMON_CLUSTER);
    }

    static bool longConnection() {
        return common::EnvParser::get<bool>(K_LONG_CONNECTION, false);
    }
};

END_CARBON_NAMESPACE(master);

#endif
