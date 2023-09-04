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

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"

namespace swift {
namespace client {
class FakeSwiftTransportClient;
class SwiftReaderAdapter;
class SwiftReaderConfig;
class SwiftReaderImpl;
class SwiftSinglePartitionReader;
class SwiftWriterImpl;

class FakeClientHelper {
public:
    FakeClientHelper();
    ~FakeClientHelper();

private:
    FakeClientHelper(const FakeClientHelper &);
    FakeClientHelper &operator=(const FakeClientHelper &);

public:
    static void makeData(FakeSwiftTransportClient *fakeClient,
                         uint32_t messageCount,
                         int64_t startId = 0,
                         int64_t factor = 3,
                         std::string data = "",
                         int64_t baseTime = 0,
                         bool mergeMsg = false);
    static FakeSwiftTransportClient *prepareTransportClientForReader(const std::string &topicName,
                                                                     uint32_t partitionId,
                                                                     bool autoAsyncCall,
                                                                     int compressFlag,
                                                                     SwiftSinglePartitionReader &reader,
                                                                     uint32_t msgCount = 0,
                                                                     int64_t msgStartId = 0);
    static SwiftReaderImpl *
    createReader(network::SwiftAdminAdapterPtr fakeAdapter, SwiftReaderConfig config, uint32_t partitionCount);

    static SwiftReaderImpl *createSimpleReader(network::SwiftAdminAdapterPtr adapter,
                                               SwiftReaderConfig config,
                                               uint32_t partitionCount,
                                               uint32_t messageCount = 3);
    static SwiftReaderImpl *
    createFakeReader(network::SwiftAdminAdapterPtr fakeAdapter, SwiftReaderConfig config, uint32_t partitionCount);

    static FakeSwiftTransportClient *getTransportClient(SwiftWriterImpl *writer, uint32_t pid);
    static FakeSwiftTransportClient *getTransportClient(SwiftReaderAdapter *reader, uint32_t offset);
    static void setTransportClientSync(SwiftWriterImpl *writer);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeClientHelper);

} // namespace client
} // namespace swift
