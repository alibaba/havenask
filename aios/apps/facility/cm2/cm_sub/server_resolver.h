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
// resolve cm server address
#ifndef __SUBSCRIBER_SERVER_RESOLVER_H_
#define __SUBSCRIBER_SERVER_RESOLVER_H_

#include "aios/apps/facility/cm2/cm_basic/leader_election/master_server.h"
#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"

namespace cm_sub {
class IServerResolver
{
public:
    virtual ~IServerResolver() { release(); }

    virtual bool init() { return true; }
    virtual void release() {}

    virtual bool resolve(std::string& ip, uint16_t& port) = 0;

protected:
    bool parseSpec(const std::string& spec, std::string& ip, uint16_t& port);
};

class ZKServerResolver : public IServerResolver
{
public:
    ZKServerResolver(const std::string& zkhost, const std::string& zkroot, int timeout,
                     /* for unittest */ int init_idx = -1, bool sub_master = false);
    ~ZKServerResolver();

    // round-robin
    virtual bool resolve(std::string& ip, uint16_t& port);

    bool isSubMaster() const { return _masterServer != NULL; }

private:
    bool resolveMaster(std::string& ip, uint16_t& port);

private:
    cm_basic::ZkWrapper _zkWrapper;
    cm_basic::ZKMasterServer* _masterServer;
    std::string _zkPath;
    int _idx;

private:
    AUTIL_LOG_DECLARE();
};

class ConfServerResolver : public IServerResolver
{
public:
    ConfServerResolver(const std::string& spec) : _spec(spec) {}

    virtual bool resolve(std::string& ip, uint16_t& port) { return parseSpec(_spec, ip, port); }

private:
    std::string _spec;
};

} // namespace cm_sub

#endif // __SUBSCRIBER_SERVER_RESOLVER_H_
