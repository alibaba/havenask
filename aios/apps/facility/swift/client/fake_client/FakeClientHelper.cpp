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
#include "swift/client/fake_client/FakeClientHelper.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "swift/client/SingleSwiftWriter.h"
#include "swift/client/SwiftReaderAdapter.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftReaderImpl.h"
#include "swift/client/SwiftSinglePartitionReader.h"
#include "swift/client/SwiftTransportAdapter.h"
#include "swift/client/SwiftTransportClient.h"
#include "swift/client/SwiftWriterImpl.h"
#include "swift/client/TransportClosure.h"
#include "swift/client/fake_client/FakeSwiftSinglePartitionReader.h"
#include "swift/client/fake_client/FakeSwiftTransportAdapter.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/common/RangeUtil.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/MessageUtil.h"

using namespace std;
using namespace autil;

using namespace swift::protocol;
using namespace swift::network;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, FakeClientHelper);

FakeClientHelper::FakeClientHelper() {}

FakeClientHelper::~FakeClientHelper() {}

void FakeClientHelper::makeData(FakeSwiftTransportClient *fakeClient,
                                uint32_t messageCount,
                                int64_t startId,
                                int64_t factor,
                                std::string data,
                                int64_t baseTime,
                                bool mergeMsg) {
    int64_t msgId = startId;
    cout << fakeClient << " make data: ";
    for (uint32_t i = 0; i < messageCount; ++i, ++msgId) {
        Message message;
        message.set_msgid(msgId);
        message.set_timestamp(baseTime + msgId * factor);
        if (data != "") {
            message.set_data(data);
        } else {
            message.set_data(std::string(100 + i, 'a'));
        }
        fakeClient->_messageVec->push_back(message);
        cout << message.msgid() << "-" << message.timestamp() << " ";
    }
    if (mergeMsg) {
        common::RangeUtil rangeUtil(1);
        vector<Message> mergedVec;
        util::MessageUtil::mergeMessage(*fakeClient->_messageVec, &rangeUtil, messageCount, mergedVec);
        *fakeClient->_messageVec = mergedVec;
    }
    cout << endl;
}

FakeSwiftTransportClient *FakeClientHelper::prepareTransportClientForReader(const string &topicName,
                                                                            uint32_t partitionId,
                                                                            bool autoAsyncCall,
                                                                            int compressFlag,
                                                                            SwiftSinglePartitionReader &reader,
                                                                            uint32_t msgCount,
                                                                            int64_t msgStartId) {
    FakeSwiftTransportAdapter<TRT_GETMESSAGE> *fakeAdapter =
        new FakeSwiftTransportAdapter<TRT_GETMESSAGE>(topicName, partitionId);
    FakeSwiftTransportClient *fakeClient = fakeAdapter->getFakeTransportClient();
    fakeClient->setAutoAsyncCall(autoAsyncCall);
    fakeClient->setCompressFlag(compressFlag);
    reader.resetTransportAdapter(fakeAdapter);
    if (msgCount > 0) {
        FakeClientHelper::makeData(fakeClient, msgCount, msgStartId);
    }
    FakeSwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME> *fakeAdapter2 =
        new FakeSwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME>(topicName, partitionId);
    FakeSwiftTransportClient *minMsgIdClient = fakeAdapter2->getFakeTransportClient();
    minMsgIdClient->setAutoAsyncCall(autoAsyncCall);
    minMsgIdClient->setCompressFlag(compressFlag);
    reader.resetMsgIdTransportAdapter(fakeAdapter2);
    if (msgCount > 0) {
        FakeClientHelper::makeData(minMsgIdClient, msgCount, msgStartId);
    }
    return fakeClient;
}

SwiftReaderImpl *
FakeClientHelper::createReader(SwiftAdminAdapterPtr fakeAdapter, SwiftReaderConfig config, uint32_t partitionCount) {

    ThreadPoolPtr threadPool(new autil::ThreadPool(2, 128));
    threadPool->start("decompress");
    TopicInfo info;
    info.set_partitioncount(partitionCount);
    SwiftReaderImpl *reader = new SwiftReaderImpl(fakeAdapter, SwiftRpcChannelManagerPtr(), info);
    if (reader->init(config) != ERROR_NONE) {
        AUTIL_LOG(WARN, "init swift reader failed.");
        return nullptr;
    }
    reader->setDecompressThreadPool(threadPool);
    return reader;
}

SwiftReaderImpl *FakeClientHelper::createFakeReader(SwiftAdminAdapterPtr fakeAdapter,
                                                    SwiftReaderConfig config,
                                                    uint32_t partitionCount) {
    SwiftReaderImpl *reader = createReader(fakeAdapter, config, partitionCount);
    if (reader == nullptr) {
        return nullptr;
    }
    for (uint32_t i = 0; i < reader->_readers.size(); ++i) {
        SwiftSinglePartitionReader *singleReader =
            new FakeSwiftSinglePartitionReader(reader->_notifier, fakeAdapter, reader->_readers[i]->_config, i);
        auto range = reader->_readers[i]->getFilterRange();
        singleReader->setFilterRange(range.first, range.second);
        delete reader->_readers[i];
        reader->_readers[i] = singleReader;
    }
    return reader;
}

SwiftReaderImpl *FakeClientHelper::createSimpleReader(SwiftAdminAdapterPtr adapter,
                                                      SwiftReaderConfig config,
                                                      uint32_t partitionCount,
                                                      uint32_t messageCount) {
    SwiftReaderImpl *reader = FakeClientHelper::createReader(adapter, config, partitionCount);
    reader->_logicTopicName = config.topicName;
    for (uint32_t i = 0; i < partitionCount; i++) {
        SwiftSinglePartitionReader *singleReader = reader->_readers[i];
        // std::cout << "create fake client for part " << i << std::endl;
        FakeClientHelper::prepareTransportClientForReader(config.topicName, i, true, 5, *singleReader, messageCount, i);
    }
    return reader;
}

FakeSwiftTransportClient *FakeClientHelper::getTransportClient(SwiftWriterImpl *writer, uint32_t pid) {
    std::map<uint32_t, SingleSwiftWriter *>::iterator iter = writer->_writers.find(pid);
    if (iter == writer->_writers.end()) {
        return NULL;
    }
    FakeSwiftTransportClient *fakeClient =
        dynamic_cast<FakeSwiftTransportClient *>(iter->second->_transportAdapter->_transportClient);
    return fakeClient;
}

FakeSwiftTransportClient *FakeClientHelper::getTransportClient(SwiftReaderAdapter *reader, uint32_t offset) {
    if (offset >= reader->_swiftReaderImpl->_readers.size()) {
        return NULL;
    }
    FakeSwiftTransportClient *fakeClient = dynamic_cast<FakeSwiftTransportClient *>(
        reader->_swiftReaderImpl->_readers[offset]->_transportAdapter->_transportClient);
    return fakeClient;
}

void FakeClientHelper::setTransportClientSync(SwiftWriterImpl *writer) {
    std::map<uint32_t, SingleSwiftWriter *>::iterator iter = writer->_writers.begin();
    for (; iter != writer->_writers.end(); iter++) {
        FakeSwiftTransportClient *fakeClient =
            dynamic_cast<FakeSwiftTransportClient *>(iter->second->_transportAdapter->_transportClient);
        if (fakeClient) {
            fakeClient->finishAsyncCall();
            fakeClient->setAutoAsyncCall();
        }
    }
}

} // namespace client
} // namespace swift
