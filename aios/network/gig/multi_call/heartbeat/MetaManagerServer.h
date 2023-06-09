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
#ifndef ISEARCH_MULTI_CALL_METAMANAGERSERVER_H
#define ISEARCH_MULTI_CALL_METAMANAGERSERVER_H

#include "aios/network/gig/multi_call/common/MetaEnv.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/proto/Heartbeat.pb.h"
#include "autil/Lock.h"
#include <map>
#include <string>

namespace multi_call {

class MetaManagerServer {
public:
    MetaManagerServer();
    ~MetaManagerServer();

private:
    MetaManagerServer(const MetaManagerServer &);
    MetaManagerServer &operator=(const MetaManagerServer &);

public:
    bool init();

    bool registerBiz(const std::string &biz_name, const BizMeta &bizMeta = {});
    bool unregisterBiz(const std::string &biz_name);
    const BizMeta *getBiz(const std::string &biz_name) const;
    bool setMeta(const std::string &biz_name, const std::string &key,
                 const std::string &value);
    const MetaKV *getMeta(const std::string &biz_name,
                          const std::string &key) const;
    bool delMeta(const std::string &biz_name, const std::string &key);
    void fillMeta(const HeartbeatRequest &request, Meta *meta);

private:
    BizMeta *getBiz(const std::string &biz_name);

private:
    MetaEnv _metaEnv;
    autil::ReadWriteLock _metaLock;
    Meta _meta;
    // map accesser for value in _meta
    std::map<std::string, BizMeta *> _bizMetaMap;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(MetaManagerServer);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_METAMANAGERSERVER_H
