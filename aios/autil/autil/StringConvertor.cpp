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
#include <stdint.h>
#include <rapidjson/internal/itoa.h>
#include <string>
#include <cstring>

#include "autil/CommonMacros.h"
#include "autil/StringConvertor.h"

namespace autil {

StringAppender::StringAppender(int32_t size) {
    idx_ = 0;
    size_ = size;
    buf_ = new char[size];
}

StringAppender::~StringAppender() {
    ARRAY_DELETE_AND_SET_NULL(buf_);
    idx_ = 0;
    size_ = 0;
}

int32_t StringAppender::normalizeSize(int32_t size) {
    return STRBUF_UNIT_SIZE * (size / STRBUF_UNIT_SIZE +
                               (size % STRBUF_UNIT_SIZE > 0 ? 1 : 0 ));
}

void StringAppender::ensureBufSize(int32_t needSize) {
    if (size_ - idx_ >= needSize) {
        return;
    }
    // resize
    int32_t newSize = normalizeSize(idx_ + needSize);
    char *newBuf = new char[newSize];
    std::memcpy(newBuf, buf_, idx_);
    ARRAY_DELETE_AND_SET_NULL(buf_);
    buf_ = newBuf;
    size_ = newSize;
}

StringAppender& StringAppender::appendString(const std::string &data) {
    ensureBufSize(data.length());
    std::memcpy(buf_ + idx_, data.data(), data.length());
    idx_ += data.length();
    return *this;
}

StringAppender& StringAppender::appendChar(const char ch) {
    ensureBufSize(1);
    buf_[idx_++] = ch;
    return *this;
}

StringAppender& StringAppender::appendBool(bool bVal) {
    ensureBufSize(1);
    buf_[idx_++] = bVal ? '1' : '0';
    return *this;
}

StringAppender& StringAppender::appendInt64(int64_t number) {
    ensureBufSize(64);
    auto buf = buf_ + idx_;
    idx_ += rapidjson::internal::i64toa(number, buf) - buf;
    return *this;
}

std::string StringAppender::toString() {
    return std::string(buf_, idx_);
}

void StringAppender::copyToString(std::string &data) {
    data.clear();
    data.insert(0, buf_, idx_);
}

}
