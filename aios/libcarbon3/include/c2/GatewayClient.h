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
#ifndef C2_GATEWAYCLIENT_H
#define C2_GATEWAYCLIENT_H

#include "hippo/proto/Client.pb.h"

namespace c2 {
   class GatewayClient {
   public:
      GatewayClient() {}
      virtual ~GatewayClient() {}

   public:
      static GatewayClient* create();

   public:
      virtual bool init(const std::string& gatewayRoot) = 0;
      virtual bool submitApplication(const hippo::proto::SubmitApplicationRequest& request,
         hippo::proto::SubmitApplicationResponse* response) = 0;

      virtual bool stopApplication(const hippo::proto::StopApplicationRequest& request,
         hippo::proto::StopApplicationResponse* response) = 0;
      virtual bool updateAppMaster(const hippo::proto::UpdateAppMasterRequest& request,
         hippo::proto::UpdateAppMasterResponse* response) = 0;
      virtual bool getAppStatus(const hippo::proto::AppStatusRequest& request, hippo::proto::AppStatusResponse* response) = 0;
   };
};
#endif  //C2_GATEWAYCLIENT_H