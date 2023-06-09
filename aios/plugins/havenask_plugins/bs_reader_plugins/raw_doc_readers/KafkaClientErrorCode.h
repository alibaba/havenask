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
#ifndef ISEARCH_BS_KAFKACLIENTERRORCODE_H
#define ISEARCH_BS_KAFKACLIENTERRORCODE_H

#include <librdkafka/rdkafkacpp.h>

namespace RdKafka {

enum ExtendErrorCode {
    ERR_EX_UNKNOWN = 10000,
    ERR_EX_CLIENT_EXCEED_TIME_STAMP_LIMIT = 10001,
};

class KafkaClientErrorCode {
public:
    KafkaClientErrorCode(RdKafka::ErrorCode ec)
        : _rdkafkaEc(ec), _rdkafkaExtendEc(RdKafka::ERR_EX_UNKNOWN), _useExtend(false)
    {}
    KafkaClientErrorCode(RdKafka::ExtendErrorCode ec)
        : _rdkafkaEc(RdKafka::ERR_UNKNOWN), _rdkafkaExtendEc(ec), _useExtend(true)
    {}
    ~KafkaClientErrorCode() {}
    bool isEqual(const KafkaClientErrorCode &rhs) const {
        if (_useExtend != rhs._useExtend) {
            return false;
        } else if (_useExtend) {
            return _rdkafkaExtendEc == rhs._rdkafkaExtendEc;
        } else {
            return _rdkafkaEc == rhs._rdkafkaEc;
        }
    }
    bool isEqual(const RdKafka::ErrorCode &rhs) const {
        if (_useExtend) {
            return false;
        } else {
            return _rdkafkaEc == rhs;
        }
    }
    bool isEqual(const RdKafka::ExtendErrorCode &rhs) const {
        if (_useExtend) {
            return _rdkafkaExtendEc == rhs;
        } else {
            return false;
        }
    }
    int code() const {
        if (_useExtend) {
            return _rdkafkaExtendEc;
        } else {
            return _rdkafkaEc;
        }
    }
    bool useExtend() {
        return _useExtend;
    }
    RdKafka::ErrorCode getErrorCode() {
        return _rdkafkaEc;
    }
    RdKafka::ExtendErrorCode getExtendErrorCode() {
        return _rdkafkaExtendEc;
    }

    friend bool operator==(const KafkaClientErrorCode &lhs, const RdKafka::ExtendErrorCode &rhs) {
        return lhs.isEqual(rhs);
    }

    friend bool operator==(const RdKafka::ExtendErrorCode &lhs, const KafkaClientErrorCode &rhs) {
        return rhs.isEqual(lhs);
    }

    friend bool operator==(const KafkaClientErrorCode &lhs, const RdKafka::ErrorCode &rhs) {
        return lhs.isEqual(rhs);
    }

    friend bool operator==(const RdKafka::ErrorCode &lhs, const KafkaClientErrorCode &rhs) {
        return rhs.isEqual(lhs);
    }

    friend bool operator==(const KafkaClientErrorCode &lhs, const KafkaClientErrorCode &rhs) {
        return lhs.isEqual(rhs);
    }

    friend bool operator!=(const KafkaClientErrorCode &lhs, const RdKafka::ExtendErrorCode &rhs) {
        return !lhs.isEqual(rhs);
    }

    friend bool operator!=(const RdKafka::ExtendErrorCode &lhs, const KafkaClientErrorCode &rhs) {
        return !rhs.isEqual(lhs);
    }

    friend bool operator!=(const KafkaClientErrorCode &lhs, const RdKafka::ErrorCode &rhs) {
        return !lhs.isEqual(rhs);
    }

    friend bool operator!=(const RdKafka::ErrorCode &lhs, const KafkaClientErrorCode &rhs) {
        return !rhs.isEqual(lhs);
    }

    friend bool operator!=(const KafkaClientErrorCode &lhs, const KafkaClientErrorCode &rhs) {
        return !lhs.isEqual(rhs);
    }

private:
    RdKafka::ErrorCode _rdkafkaEc;
    RdKafka::ExtendErrorCode _rdkafkaExtendEc;
    bool _useExtend;
};

inline std::string err2str(RdKafka::KafkaClientErrorCode ec) {
    if (!ec.useExtend()) {
        return RdKafka::err2str(ec.getErrorCode());
    } else {
        return std::string("ExtendErrorCode:") + std::to_string(ec.getExtendErrorCode());
    }
}

}

#endif //ISEARCH_BS_KAFKACLIENTERRORCODE_H
