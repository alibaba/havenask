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
#include "access_log/AccessLogAccessorFactory.h"

#include "access_log/local/LocalAccessLogAccessor.h"
#include "access_log/swift/SwiftAccessLogAccessor.h"
#include "autil/Log.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(access_log, AccessLogAccessorFactory);

namespace access_log {

AccessLogAccessorFactory::AccessLogAccessorFactory() {}

AccessLogAccessorFactory::~AccessLogAccessorFactory() {}

AccessLogAccessor *AccessLogAccessorFactory::getAccessor(const std::string &logName,
                                                         const std::string &typeName,
                                                         const autil::legacy::Any &config,
                                                         const autil::PartitionRange &partRange) {
    if (typeName == LOCAL_TYPE) {
        return new LocalAccessLogAccessor(logName);
    } else if (typeName == SWIFT_TYPE) {
        return new SwiftAccessLogAccessor(logName, config, partRange);
    } else {
        AUTIL_LOG(ERROR, "type %s not supported", typeName.c_str());
        return nullptr;
    }
}

} // namespace access_log
