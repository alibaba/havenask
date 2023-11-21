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
#pragma once

#include <string>

#include "build_service/util/Log.h"
#include "build_service_tasks/channel/ChannelBase.h"
#include "build_service_tasks/channel/Master.pb.h"
#include "fslib/util/FileUtil.h"

namespace build_service { namespace task_base {

class MadroxChannel : public ChannelBase
{
public:
    MadroxChannel(const std::string& madroxMasterRoot)
    {
        _hostPath = fslib::util::FileUtil::joinFilePath(madroxMasterRoot, "master");
    }
    virtual ~MadroxChannel() {}

public:
    virtual bool GetStatus(const ::madrox::proto::GetStatusRequest& request,
                           ::madrox::proto::GetStatusResponse& response);

    virtual bool UpdateRequest(const ::madrox::proto::UpdateRequest& request,
                               ::madrox::proto::UpdateResponse& response);

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::task_base
