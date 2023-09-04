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
#ifndef HIPPO_HIPPOCLIENT_H
#define HIPPO_HIPPOCLIENT_H

#include "hippo/proto/Client.pb.h"

namespace hippo {

class HippoClient
{
public:
    HippoClient() {}
    virtual ~HippoClient() {}
public:
    static HippoClient *create();
public:
    virtual bool init(const std::string &hippoRoot) = 0;
    virtual bool submitApplication(const proto::SubmitApplicationRequest &request,
                                   proto::SubmitApplicationResponse *response) = 0;
    virtual bool stopApplication(const proto::StopApplicationRequest &request,
                                 proto::StopApplicationResponse *response) = 0;
    virtual bool forceKillApplication(const proto::ForceKillApplicationRequest &request,
            proto::ForceKillApplicationResponse *response) = 0;
    virtual bool getServiceStatus(const proto::ServiceStatusRequest &request,
                                  proto::ServiceStatusResponse *response) = 0;
    virtual bool getSlaveStatus(const proto::SlaveStatusRequest &request,
                                proto::SlaveStatusResponse *response) = 0;
    virtual bool getAppSummary(const proto::AppSummaryRequest request,
                               proto::AppSummaryResponse *response) = 0;
    virtual bool getAppStatus(const proto::AppStatusRequest &request,
                              proto::AppStatusResponse *response) = 0;
    virtual bool getAppResourcePreference(const proto::ResourcePreferenceRequest &request,
            proto::ResourcePreferenceResponse *response) = 0;
    virtual bool updateAppResourcePreference(
            const proto::UpdateResourcePreferenceRequest &request,
            proto::UpdateResourcePreferenceResponse *response) = 0;
    virtual bool updateApplication(const proto::UpdateApplicationRequest &request,
                                   proto::UpdateApplicationResponse *response) = 0;
    virtual bool addQueue(const proto::AddQueueRequest &request,
                          proto::AddQueueResponse *response) = 0;
    virtual bool delQueue(const proto::DelQueueRequest &request,
                          proto::DelQueueResponse *response) = 0;
    virtual bool updateQueue(const proto::UpdateQueueRequest &request,
                             proto::UpdateQueueResponse *response) = 0;
    virtual bool getQueueStatus(const proto::QueueStatusRequest &request,
                                proto::QueueStatusResponse *response) = 0;
    virtual bool freezeApplication(const proto::FreezeApplicationRequest &request,
                                   proto::FreezeApplicationResponse *response) = 0;
    virtual bool unfreezeApplication(const proto::UnFreezeApplicationRequest &request,
            proto::UnFreezeApplicationResponse *response) = 0;
    virtual bool freezeAllApplication(const proto::FreezeAllApplicationRequest &request,
            proto::FreezeAllApplicationResponse *response) = 0;
    virtual bool unfreezeAllApplication(const proto::UnFreezeAllApplicationRequest &request,
            proto::UnFreezeAllApplicationResponse *response) = 0;
    virtual bool updateAppMaster(const proto::UpdateAppMasterRequest &request,
                                 proto::UpdateAppMasterResponse *response) = 0;
    virtual bool updateMasterScheduleSwitch(const proto::UpdateMasterOptionsRequest &request,
            proto::UpdateMasterOptionsResponse *response) = 0;
};

};

#endif //HIPPO_HIPPOCLIENT_H
