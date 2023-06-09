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
#include <string>
#include <vector>
#include <iostream>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <fstream>
#include "autil/StringUtil.h"
#include "kmonitor/client/common/Common.h"
#include "autil/Log.h"
#include "kmonitor/client/net/LogFileAgentClient.h"
#include "kmonitor/client/common/Util.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonmetrics, LogFileAgentClient);

using namespace std;


LogFileAgentClient::LogFileAgentClient() {
}

LogFileAgentClient::~LogFileAgentClient() {
    Close();
}

bool LogFileAgentClient::Init() {
    return true;
}

bool LogFileAgentClient::Started() const {
    return started_;
}

void LogFileAgentClient::Close() {
    started_ = false;
}


bool LogFileAgentClient::AppendBatch(const BatchFlumeEventPtr &batchEvents) {
    if (!Started()) {
        ReConnect();
    }
    std::vector<ThriftFlumeEvent*> events = *(batchEvents->Get());
    for(auto event : events) {
        string eventBody = event->GetBody();
        AUTIL_LOG(INFO, "%s", eventBody.c_str());
    }
    return true;
}

bool LogFileAgentClient::ReConnect() {
    Close();
    started_ = true;
    return true;
}

END_KMONITOR_NAMESPACE(kmonitor);

