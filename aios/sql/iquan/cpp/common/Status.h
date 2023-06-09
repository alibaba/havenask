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

#include <memory>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "iquan/common/Common.h"

namespace iquan {

class Status {
public:
    Status() {}
    Status(int code, const std::string &msg);
    Status(const Status &s);
    void operator=(const Status &s);

    static Status OK() { return Status(); }

    bool ok() const { return _state == nullptr || _state->code == IQUAN_OK; }

    int code() const { return _state == nullptr ? IQUAN_OK : _state->code; }

    const std::string &errorMessage() const { return ok() ? emptyString() : _state->msg; }

    bool operator==(const Status &s) const;
    bool operator!=(const Status &s) const;

private:
    static const std::string &emptyString();
    struct State {
        int code;
        std::string msg;
    };
    std::unique_ptr<State> _state;
};

inline Status::Status(const Status &s) : _state((s._state == nullptr) ? nullptr : new State(*s._state)) {}

inline void Status::operator=(const Status &s) {
    if (_state != s._state) {
        if (s._state == nullptr) {
            _state = nullptr;
        } else {
            _state = std::unique_ptr<State>(new State(*s._state));
        }
    }
}

inline bool Status::operator==(const Status &s) const {
    return (_state == s._state || (code() == s.code() && errorMessage() == s.errorMessage()));
}

inline bool Status::operator!=(const Status &s) const { return !(*this == s); }

class StatusJsonWrapper : public autil::legacy::Jsonizable {
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        if (json.GetMode() == TO_JSON) {
            std::string errMsg = status.errorMessage();
            int code = status.code();
            json.Jsonize("error_message", errMsg);
            json.Jsonize("error_code", code);
        }
    }

public:
    Status status;
};

} // namespace iquan
