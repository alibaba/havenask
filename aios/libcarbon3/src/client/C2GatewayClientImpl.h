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
#ifndef CARBON_C2GATEWAYCLIENTIMPL_H
#define CARBON_C2GATEWAYCLIENTIMPL_H

#include "c2/GatewayClient.h"
#include "common/common.h"
#include "carbon/Log.h"
#include "aios/network/anet/anet.h"
#include "master/HttpClient.h"
#include "google/protobuf/service.h"
#include "hippo/HippoClient.h"

BEGIN_C2_NAMESPACE(gateway);

class C2GatewayClientImpl : public GatewayClient, public hippo::HippoClient {
public:
   C2GatewayClientImpl();
   ~C2GatewayClientImpl();

private:
   template <typename T>
   struct ArpcOperation : public autil::legacy::Jsonizable {
   public:
      void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
         JSON_FIELD(op);
         JSON_FIELD(path);
         JSON_FIELD(value);
         JSON_FIELD(metaInfo);
      }
      std::string op;
      std::string path;
      T value;
      std::map<std::string, std::string> metaInfo;
   };
   struct GatewayResponse : public autil::legacy::Jsonizable {
      JSONIZE() {
         JSON_FIELD(status);
         JSON_FIELD(data);
         JSON_FIELD(desc);
      }
      bool isSuccess() const { return status == 0; }
      int status;
      autil::legacy::json::JsonMap data;
      string desc;
   };
public:
   /* override */ bool init(const std::string& gatewayRoot);
   /* override */ bool submitApplication(const hippo::proto::SubmitApplicationRequest& request,
      hippo::proto::SubmitApplicationResponse* response);
   /* override */ bool stopApplication(const hippo::proto::StopApplicationRequest& request,
      hippo::proto::StopApplicationResponse* response);
   /* override */ bool getAppStatus(const hippo::proto::AppStatusRequest& request, hippo::proto::AppStatusResponse* response);
   /* override */ bool updateAppMaster(const hippo::proto::UpdateAppMasterRequest& request, hippo::proto::UpdateAppMasterResponse* response);


    virtual bool forceKillApplication(const hippo::proto::ForceKillApplicationRequest &request,
            hippo::proto::ForceKillApplicationResponse *response) { return false; };
    virtual bool getServiceStatus(const hippo::proto::ServiceStatusRequest &request,
                                  hippo::proto::ServiceStatusResponse *response) { return false; };
    virtual bool getSlaveStatus(const hippo::proto::SlaveStatusRequest &request,
                                hippo::proto::SlaveStatusResponse *response) { return false; };
    virtual bool getAppSummary(const hippo::proto::AppSummaryRequest request,
                               hippo::proto::AppSummaryResponse *response) { return false; };
    virtual bool getAppResourcePreference(const hippo::proto::ResourcePreferenceRequest &request,
            hippo::proto::ResourcePreferenceResponse *response) { return false; };
    virtual bool updateAppResourcePreference(
            const hippo::proto::UpdateResourcePreferenceRequest &request,
            hippo::proto::UpdateResourcePreferenceResponse *response) { return false; };
    virtual bool updateApplication(const hippo::proto::UpdateApplicationRequest &request,
                                   hippo::proto::UpdateApplicationResponse *response) { return false; };
    virtual bool addQueue(const hippo::proto::AddQueueRequest &request,
                          hippo::proto::AddQueueResponse *response) { return false; };
    virtual bool delQueue(const hippo::proto::DelQueueRequest &request,
                          hippo::proto::DelQueueResponse *response) { return false; };
    virtual bool updateQueue(const hippo::proto::UpdateQueueRequest &request,
                             hippo::proto::UpdateQueueResponse *response) { return false; };
    virtual bool getQueueStatus(const hippo::proto::QueueStatusRequest &request,
                                hippo::proto::QueueStatusResponse *response) { return false; };
    virtual bool freezeApplication(const hippo::proto::FreezeApplicationRequest &request,
                                   hippo::proto::FreezeApplicationResponse *response) { return false; };
    virtual bool unfreezeApplication(const hippo::proto::UnFreezeApplicationRequest &request,
            hippo::proto::UnFreezeApplicationResponse *response) { return false; };
    virtual bool freezeAllApplication(const hippo::proto::FreezeAllApplicationRequest &request,
            hippo::proto::FreezeAllApplicationResponse *response) { return false; };
    virtual bool unfreezeAllApplication(const hippo::proto::UnFreezeAllApplicationRequest &request,
            hippo::proto::UnFreezeAllApplicationResponse *response) { return false; };
    virtual bool updateMasterScheduleSwitch(const hippo::proto::UpdateMasterOptionsRequest &request,
            hippo::proto::UpdateMasterOptionsResponse *response) { return false; };
private:
   template <typename T>
   bool doRequest(const std::string& apiHint, anet::HTTPPacket::HTTPMethod method, const std::string& api, const ArpcOperation<T> req, ::google::protobuf::Message* result, bool verbose = true);

   template <typename T>
   ArpcOperation<T> newArpcOperation(const std::string path, T value);

private:
   carbon::master::HttpClientPtr _http;       // post to gateway
   std::string _host;
   std::string _platform;
   std::string _clusterId;
   CARBON_LOG_DECLARE();
};

END_C2_NAMESPACE(gateway);
#endif  // CARBON_C2GATEWAYCLIENTIMPL_H
