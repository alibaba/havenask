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
#include "autil/Log.h"
#include "access_log/swift/SwiftAccessLogConfig.h"

using namespace std;
using namespace swift::client;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, SwiftAccessLogConfig);

namespace access_log {

SwiftAccessLogConfig::SwiftAccessLogConfig() {
}

SwiftAccessLogConfig::~SwiftAccessLogConfig() {
}

void SwiftAccessLogConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("clientConfig", _clientConfig, _clientConfig);
    json.Jsonize("readerConfig", _readerConfig, _readerConfig);
    json.Jsonize("writerConfig", _writerConfig, _writerConfig);            
}

bool SwiftAccessLogConfig::isValidate() {
    return _writerConfig.isValidate();
}

}
