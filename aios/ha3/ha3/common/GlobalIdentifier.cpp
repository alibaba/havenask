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
#include "ha3/common/GlobalIdentifier.h"

#include "alog/Logger.h"
#include "autil/DataBuffer.h"

#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, GlobalIdentifier);

void ExtraDocIdentifier::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(primaryKey);
    dataBuffer.write(fullIndexVersion);
    dataBuffer.write(indexVersion);
    dataBuffer.write(ip);
}

void ExtraDocIdentifier::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(primaryKey);
    dataBuffer.read(fullIndexVersion);
    dataBuffer.read(indexVersion);
    dataBuffer.read(ip);
}
//////////////////////////////////////////////////////////////////////////////
void GlobalIdentifier::serialize(autil::DataBuffer &dataBuffer) const {
    AUTIL_LOG(DEBUG, "GlobalIdentifier serialize.");
    dataBuffer.write(_docId);
    dataBuffer.write(_hashId);
    dataBuffer.write(_clusterId);
    dataBuffer.write(_extraDocIdentifier);
    dataBuffer.write(_pos);
}

void GlobalIdentifier::deserialize(autil::DataBuffer &dataBuffer) {
    AUTIL_LOG(DEBUG, "GlobalIdentifier deserialize.");
    dataBuffer.read(_docId);
    dataBuffer.read(_hashId);
    dataBuffer.read(_clusterId);
    dataBuffer.read(_extraDocIdentifier);
    dataBuffer.read(_pos);
}

} // namespace common
} // namespace isearch
