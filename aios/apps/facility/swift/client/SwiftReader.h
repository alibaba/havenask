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
#include <vector>

#include "autil/ThreadPool.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace common {
class SchemaReader;
} // namespace common
namespace protocol {
class Message;
class Messages;
class ReaderProgress;
} // namespace protocol
} // namespace swift

namespace swift {
namespace client {
class SwiftReaderConfig;

class SwiftReader {
public:
    SwiftReader();
    virtual ~SwiftReader();

public:
    virtual protocol::ErrorCode init(const SwiftReaderConfig &config) = 0;
    /**
     * read()  only used when read single partition
     * @return ErrorCode, ERROR_NONE:                           succeeded
     * @return ErrorCode, ERROR_CLIENT_EXCEED_TIME_STAMP_LIMIT  next message timestamp exceed setted
     * @return ErrorCode, ERROR_CLIENT_NO_MORE_MESSAGE:         no more message now
     * @return ErrorCode, ERROR_CLIENT_READ_MESSAGE_TIMEOUT:    get message timeout
     * @return ErrorCode, ERROR_CLIENT_SYS_STOPPED:             swift server been stopped
     * @return ErrorCode, ERROR_CLIENT_GET_ADMIN_INFO_FAILED:   failed to get admin info from zookeeper
     * @return ErrorCode, ERROR_CLIENT_ARPC_ERROR:              rpc to swift admin failed
     * @return ErrorCode, ERROR_CLIENT_INVALID_RESPONSE:        invalid response
     * @return ErrorCode, ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED: failed to get broker address
     * @return ErrorCode, ERROR_BROKER_BUSY:                    swift broker is busy currently
     * @return ErrorCode, ERROR_BROKER_STOPPED:                 swift broker been stopped
     * @return ErrorCode, ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND: topic or partition not found in broker
     * @return ErrorCode, ERROR_BROKER_REQUEST_INVALID:         request invalid
     * @return ErrorCode, ERROR_ADMIN_NOT_LEADER:               swift admin is not leader, such as swift admin lost
     *heartbeat with zookeeper
     * @return ErrorCode, ERROR_ADMIN_IP_SET_INVALID:
     * @return ErrorCode, ERROR_ADMIN_TOPIC_NOT_EXISTED:        topic not exist in admin
     * @return ErrorCode, ERROR_ADMIN_PARTITION_NOT_EXISTED:    partition not exist in admin
     * @return ErrorCode, ERROR_ADMIN_INVALID_PARAMETER:        invalid request
     * @return ErrorCode, ERROR_SEALED_TOPIC_READ_FINISH:       sealed topic read finish
     **/
    virtual protocol::ErrorCode read(protocol::Message &msg,
                                     int64_t timeout = 3 * 1000000); // 3s
    /**
     *read multi messages, only used when read single partition
     */
    virtual protocol::ErrorCode read(protocol::Messages &msgs, int64_t timeout = 3 * 1000000);
    /**
     * read(), used when read from multi partitions
     * @parameter timeStamp represents the checkpoint after read current message.
     * If reader has error, the value of current checkpoint is last one.
     **/
    virtual protocol::ErrorCode read(int64_t &timeStamp,
                                     protocol::Message &msg,
                                     int64_t timeout = 3 * 1000000) = 0; // 3 s
    /**
     * read multi message
     **/
    virtual protocol::ErrorCode read(int64_t &timeStamp,
                                     protocol::Messages &msgs,
                                     int64_t timeout = 3 * 1000000); // 3 s
    /**
     * seekByMessageId()
     * @return ErrorCode, ERROR_NONE:                           succeeded
     **/
    virtual protocol::ErrorCode seekByMessageId(int64_t msgId) = 0;

    /**
     * seekByTimestamp()
     * @parameter force used when read from multi partitions
     * @return ErrorCode, ERROR_NONE:                           succeeded
     * @return ErrorCode, ERROR_CLIENT_SYS_STOPPED:             swift server been stopped
     * @return ErrorCode, ERROR_CLIENT_GET_ADMIN_INFO_FAILED:   failed to get admin info from zookeeper
     * @return ErrorCode, ERROR_CLIENT_ARPC_ERROR:              rpc to swift admin failed
     * @return ErrorCode, ERROR_CLIENT_INVALID_RESPONSE:        invalid response
     * @return ErrorCode, ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED: failed to get broker address
     * @return ErrorCode, ERROR_BROKER_BUSY:                    swift broker is busy currently
     * @return ErrorCode, ERROR_BROKER_STOPPED:                 swift broker been stopped
     * @return ErrorCode, ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND: topic or partition not found in broker
     * @return ErrorCode, ERROR_BROKER_REQUEST_INVALID:         request invalid
     * @return ErrorCode, ERROR_ADMIN_NOT_LEADER:               swift admin is not leader, such as swift admin lost
     *heartbeat with zookeeper
     * @return ErrorCode, ERROR_ADMIN_IP_SET_INVALID:
     * @return ErrorCode, ERROR_ADMIN_TOPIC_NOT_EXISTED:        topic not exist in admin
     * @return ErrorCode, ERROR_ADMIN_PARTITION_NOT_EXISTED:    partition not exist in admin
     * @return ErrorCode, ERROR_ADMIN_INVALID_PARAMETER:        invalid request
     * @return ErrorCode, ERROR_BROKER_OPEN_FILE:               mem meta and real file inconsistent, using currentTime
     *to seek again //ls
     **/
    virtual protocol::ErrorCode seekByTimestamp(int64_t timestamp, bool force = false) = 0;

    virtual protocol::ErrorCode seekByProgress(const protocol::ReaderProgress &progress, bool force = false) = 0;

    /**
     * checkCurrentError(), used to get detailed error code and error msg
     * @return ErrorCode, ERROR_NONE:                           succeeded
     * @return ErrorCode, ERROR_CLIENT_SYS_STOPPED:             swift server been stopped
     * @return ErrorCode, ERROR_CLIENT_GET_ADMIN_INFO_FAILED:   failed to get admin info from zookeeper
     * @return ErrorCode, ERROR_CLIENT_ARPC_ERROR:              rpc to swift admin failed
     * @return ErrorCode, ERROR_CLIENT_INVALID_RESPONSE:        invalid response
     * @return ErrorCode, ERROR_CLIENT_GET_BROKER_ADDRESS_FAILED: failed to get broker address
     * @return ErrorCode, ERROR_BROKER_BUSY:                    swift broker is busy currently
     * @return ErrorCode, ERROR_BROKER_STOPPED:                 swift broker been stopped
     * @return ErrorCode, ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND: topic or partition not found in broker
     * @return ErrorCode, ERROR_BROKER_REQUEST_INVALID:         request invalid
     * @return ErrorCode, ERROR_ADMIN_NOT_LEADER:               swift admin is not leader, such as swift admin lost
     *heartbeat with zookeeper
     * @return ErrorCode, ERROR_ADMIN_IP_SET_INVALID:
     * @return ErrorCode, ERROR_ADMIN_TOPIC_NOT_EXISTED:        topic not exist in admin
     * @return ErrorCode, ERROR_ADMIN_PARTITION_NOT_EXISTED:    partition not exist in admin
     * @return ErrorCode, ERROR_ADMIN_INVALID_PARAMETER:        invalid request
     **/
    virtual void checkCurrentError(protocol::ErrorCode &errorCode, std::string &errorMsg) const = 0;

    virtual SwiftPartitionStatus getSwiftPartitionStatus() = 0;

    virtual void setRequiredFieldNames(const std::vector<std::string> &fieldNames) = 0;

    virtual void setFieldFilterDesc(const std::string &filterDesc) = 0;

    /**
     * @acceptTimestamp: the min partition timestamp, accecptTimestamp >= timestampLimit
     *                   message's timestamp  <= acceptTimestamp can be readed.
     */
    virtual void setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) = 0;
    virtual std::vector<std::string> getRequiredFieldNames() = 0;
    virtual bool updateCommittedCheckpoint(int64_t checkpoint) {
        (void)checkpoint;
        return true;
    }
    virtual void setDecompressThreadPool(const autil::ThreadPoolPtr &decompressThreadPool) {
        _decompressThreadPool = decompressThreadPool;
    }
    /**
     * get schema reader to parse schema message data, only for schema topic
     */
    virtual common::SchemaReader *getSchemaReader(const char *data, protocol::ErrorCode &ec) = 0;

    virtual protocol::ErrorCode getReaderProgress(protocol::ReaderProgress &progress) const = 0;
    virtual int64_t getCheckpointTimestamp() const = 0;

protected:
    autil::ThreadPoolPtr _decompressThreadPool;
};

extern SwiftReader *createSwiftReader(network::SwiftAdminAdapterPtr adminAdapter,
                                      network::SwiftRpcChannelManagerPtr channelManager); // __attribute__((weak));

SWIFT_TYPEDEF_PTR(SwiftReader);

} // namespace client
} // namespace swift
