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
#include "swift/client/fake_client/SwiftWriterImplMock.h"

#include <functional>
#include <iosfwd>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "swift/client/SingleSwiftWriter.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/client/TransportClosure.h"
#include "swift/client/fake_client/FakeSwiftTransportAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"

namespace swift {
namespace protocol {
class TopicInfo;
} // namespace protocol
} // namespace swift

using namespace std;
using namespace swift::util;
using namespace swift::network;

namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, SwiftWriterImplMock);

SwiftWriterImplMock::SwiftWriterImplMock(SwiftAdminAdapterPtr adapter, const protocol::TopicInfo &topicInfo)
    : SwiftWriterImpl(adapter, SwiftRpcChannelManagerPtr(), autil::ThreadPoolPtr(), topicInfo) {}

SwiftWriterImplMock::~SwiftWriterImplMock() {}

SingleSwiftWriter *SwiftWriterImplMock::insertSingleWriter(uint32_t pid) {
    autil::ScopedWriteLock lock(_mapLock);
    // double check exist for thread safe.
    std::map<uint32_t, SingleSwiftWriter *>::const_iterator it = _writers.find(pid);
    if (it != _writers.end()) {
        return it->second;
    }

    SingleSwiftWriter *writer =
        new SingleSwiftWriter(_adminAdapter, _channelManager, pid, _config, _blockPool, _mergeThreadPool);
    _writers[pid] = writer;
    FakeSwiftTransportAdapter<TRT_SENDMESSAGE> *adapter =
        new FakeSwiftTransportAdapter<TRT_SENDMESSAGE>(_config.topicName, pid);
    DELETE_AND_SET_NULL(writer->_transportAdapter);
    writer->_transportAdapter = adapter;
    adapter->setCallBackFunc(std::bind(&SwiftWriterImpl::setTopicChanged, this, std::placeholders::_1));
    _clientMap[pid] = adapter->getFakeTransportClient();
    return writer;
}

SingleSwiftWriter *SwiftWriterImplMock::getWriter(uint32_t pid) { return getSingleWriter(pid); }

} // namespace client
} // namespace swift
