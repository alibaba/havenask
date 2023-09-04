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
#ifndef ISEARCH_BS_SERVICEINFOFILTER_H
#define ISEARCH_BS_SERVICEINFOFILTER_H

#include "autil/legacy/json.h"
#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/util/Log.h"
#include "worker_framework/ZkState.h"

namespace build_service { namespace admin {

class ServiceInfoFilter
{
public:
    ServiceInfoFilter() : _zkWrapper(NULL) {}

    ~ServiceInfoFilter();

private:
    ServiceInfoFilter(const ServiceInfoFilter&);
    ServiceInfoFilter& operator=(const ServiceInfoFilter&);

public:
    bool init(cm_basic::ZkWrapper* zkWrapper, const std::string& zkRoot);
    void filter(const proto::GenerationInfo& gsInfo, proto::GenerationInfo& filteredGsInfo) const;
    bool update(const std::string& templateStr);

private:
    bool doFilter(google::protobuf::Message* msg, const autil::legacy::json::JsonMap& jsonMap) const;

private:
    cm_basic::ZkWrapper* _zkWrapper;
    std::string _zkRoot;
    autil::legacy::json::JsonMap _templateMap;
    std::string _templateStr;
    mutable autil::ReadWriteLock _lock;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ServiceInfoFilter);

}} // namespace build_service::admin

#endif // ISEARCH_BS_SERVICEINFOFILTER_H
