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

#include <vector>
#include <string>
#include "kmonitor/client/common/Common.h"
#include "autil/Log.h"
#include "kmonitor/client/net/BatchFlumeEvent.h"
#include "kmonitor/client/net/BaseAgentClient.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class LogFileAgentClient : public BaseAgentClient{
 public:
   LogFileAgentClient();
   ~LogFileAgentClient();

   bool AppendBatch(const BatchFlumeEventPtr &events) override;
   bool Started() const override;
   bool Init() override;
   void Close() override;
   bool ReConnect() override;
private:
   bool started_;

private:
   AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);
