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
/**
 * File name: defaultpacket.cpp
 * Author: zhangli
 * Create time: 2008-12-25 11:10:50
 * $Id$
 *
 * Description: ***add description here***
 *
 */

#include "aios/network/anet/defaultpacket.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "aios/network/anet/databuffer.h"
#include "aios/network/anet/packet.h"

namespace anet {
DefaultPacket::DefaultPacket() {
    _body = NULL;
    _bodyLength = 0;
    _capacity = 0;
}

DefaultPacket::~DefaultPacket() {
    if (_body)
        ::free(_body);
}

bool DefaultPacket::setBody(const char *str, size_t length) {
    if (0 == length) {
        if (_body)
            ::free(_body);
        _body = NULL;
        _bodyLength = 0;
        _capacity = 0;
        return true;
    }
    size_t oriLen = _bodyLength;
    _bodyLength = 0;
    if (appendBody(str, length)) {
        return true;
    } else {
        _bodyLength = oriLen;
        return false;
    }
}

bool DefaultPacket::appendBody(const char *str, size_t length) {
    if (0 == length || NULL == str) {
        return false;
    }

    if (_capacity - _bodyLength < length) {
        size_t tmpSize = (_capacity * 2 - _bodyLength) >= length ? _capacity * 2 : length * 2 + _bodyLength;
        if (!setCapacity(tmpSize)) {
            return false;
        }
    }
    memcpy(_body + _bodyLength, str, length);
    _bodyLength += length;
    return true;
}

const char *DefaultPacket::getBody(size_t &length) const {
    length = _bodyLength;
    return _body;
}

char *DefaultPacket::getBody(size_t &length) {
    length = _bodyLength;
    return _body;
}

const char *DefaultPacket::getBody() const { return _body; }

char *DefaultPacket::getBody() { return _body; }

size_t DefaultPacket::getBodyLen() const { return _bodyLength; }

int64_t DefaultPacket::getSpaceUsed() { return (sizeof(PacketHeader) + sizeof(DefaultPacket) + getBodyLen()); }

bool DefaultPacket::setCapacity(size_t capacity) {
    if (capacity < _bodyLength) {
        return false;
    }
    if (capacity == _capacity) {
        return true;
    }
    char *tmp = (char *)realloc(_body, capacity);
    if (!tmp) {
        return false;
    }
    _body = tmp;
    _capacity = capacity;
    return true;
}

size_t DefaultPacket::getCapacity() { return _capacity; }

bool DefaultPacket::encode(DataBuffer *output) {
    if (_bodyLength > 0) {
        output->writeBytes(_body, _bodyLength);
    }
    return true;
}

bool DefaultPacket::decode(DataBuffer *input, PacketHeader *header) {
    assert(input->getDataLen() >= header->_dataLen);
    bool rc = setBody(input->getData(), header->_dataLen);
    input->drainData(header->_dataLen);
    return rc;
}

} /*end namespace anet*/
