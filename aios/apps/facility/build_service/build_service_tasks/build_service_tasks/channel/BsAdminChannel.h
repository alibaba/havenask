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
#ifndef ISEARCH_BS_BSADMINCHANNEL_H
#define ISEARCH_BS_BSADMINCHANNEL_H

#include "build_service/proto/Admin.pb.h"
#include "build_service_tasks/channel/ChannelBase.h"
#include "fslib/util/FileUtil.h"

namespace build_service { namespace task_base {

class BsAdminChannel : public ChannelBase
{
public:
    BsAdminChannel(const std::string& bsAdminRoot)
    {
        _hostPath = fslib::util::FileUtil::joinFilePath(bsAdminRoot, "admin");
    }
    virtual ~BsAdminChannel() {}

public:
    virtual bool GetServiceInfo(const build_service::proto::ServiceInfoRequest& request,
                                build_service::proto::ServiceInfoResponse& response);

    virtual bool CreateSnapshot(const build_service::proto::CreateSnapshotRequest& request,
                                build_service::proto::InformResponse& response);

    virtual bool RemoveSnapshot(const build_service::proto::RemoveSnapshotRequest& request,
                                build_service::proto::InformResponse& response);

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::task_base

#endif // ISEARCH_BS_BSADMINCHANNEL_H
