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
#ifndef ISEARCH_ACCESS_LOG_SWIFTACCESSLOGCONFIG_H
#define ISEARCH_ACCESS_LOG_SWIFTACCESSLOGCONFIG_H

#include "autil/legacy/jsonizable.h"
#include "swift/client/SwiftClientConfig.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/client/SwiftReaderConfig.h"
#include "access_log/common.h"

namespace access_log {

/*
swift config template:
{
    "zkPath": "zfs://xxx",
    "clientConfig": {
        "retryTimes": 123,
        "retryTimeInterval": 123,
        "useFollowerAdmin": true,      //default true
        "adminTimeout": 123,
        "refreshTime": 123,
        "maxWriteBufferSizeMb": 123,
        "tempWriteBufferPercent": 123,
        "bufferBlockSizeKb": 123,
        "mergeMessageThreadNum": 123,
    }
    "readerConfig": {
        "readPartVec": [],
        "fatalErrorTimeLimit": 123,
        "fatalErrorReportInterval": 123,
        "consumptionRequestBytesLimit": 123,
        "oneRequestReadCount": 123,
        "readBufferSize": 123,
        "retryGetMsgInterval": 123,
        "partitionStatusRefreshInterval": 123,
        "needCompress": false,         //default false
        "canDecompress": true,         //default true
        "requiredFields": [],
        "fieldFilterDesc": "",
        "messageFormat": 123,
        "batchReadCount": 123,
        "readAll": true,               //default false
        "mergeMessage": 123
    },
    "writerConfig": {
        "isSynchronous": false;         //default false
        "retryTimes": 123,
        "retryTimeInterval": 123,
        "oneRequestSendByteCount": 123,
        "maxBufferSize": 123,
        "maxKeepInBufferTime": 123,
        "sendRequestLoopInterval": 123,
        "waitLoopInterval": 123,
        "brokerBusyWaitIntervalMin": 123,
        "brokerBusyWaitIntervalMax": 123,
        "functionChain": "",
        "waitFinishedWriterTime": 123,
        "compressThresholdInBytes": 123,
        "commitDetectionInterval": 123,
        "compress": false,               //default false
        "compressMsg": false,            //default false
        "compressMsgInBroker": false,    //default false
        "safeMode": true,                //default true
        "syncSendTimeout": 123,
        "asyncSendTimeout": 123,
        "messageFormat": 123,
        "mergeMsg": false,               //default false
        "reserved": 123,
        "mergeThresholdInSize": 123,
    }
}
 */

class SwiftAccessLogConfig : public autil::legacy::Jsonizable {
public:
    SwiftAccessLogConfig();
    ~SwiftAccessLogConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);

    bool isValidate();

public:
    swift::client::SwiftClientConfig _clientConfig;
    swift::client::SwiftReaderConfig _readerConfig;
    swift::client::SwiftWriterConfig _writerConfig;
};

}

#endif //ISEARCH_ACCESS_LOG_SWIFTACCESSLOGCONFIG_H
