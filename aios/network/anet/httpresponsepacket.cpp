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
#include "aios/network/anet/httpresponsepacket.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <tr1/unordered_map>
#include <utility>

#include "aios/network/anet/databuffer.h"

namespace anet {
class PacketHeader;

/*
 * 构造函数
 */
HttpResponsePacket::HttpResponsePacket() {
    _status = true;
    _body = NULL;
    _bodyLen = 0;
    _isKeepAlive = false;
}

/*
 * 析构函数
 */
HttpResponsePacket::~HttpResponsePacket() {
    if (_body) {
        ::free(_body);
    }
}

/*
 * 组装
 */
bool HttpResponsePacket::encode(DataBuffer *output) {
    if (_status) { // HTTP/1.1 200 OK
        output->writeBytes(ANET_HTTP_STATUS_OK, strlen(ANET_HTTP_STATUS_OK));
    } else { // HTTP/1.1 404 Not Found
        output->writeBytes(ANET_HTTP_STATUS_NOTFOUND, strlen(ANET_HTTP_STATUS_NOTFOUND));
    }
    //固定字段
    if (_isKeepAlive) {
        output->writeBytes(ANET_HTTP_KEEP_ALIVE, strlen(ANET_HTTP_KEEP_ALIVE));
    } else {
        output->writeBytes(ANET_HTTP_CONN_CLOSE, strlen(ANET_HTTP_CONN_CLOSE));
    }
    if (_headerMap.find("Content-Type") == _headerMap.end()) {
        output->writeBytes(ANET_HTTP_CONTENT_TYPE, strlen(ANET_HTTP_CONTENT_TYPE));
    }
    char tmp[64];
    int len = sprintf(tmp, ANET_HTTP_CONTENT_LENGTH, _bodyLen);
    output->writeBytes(tmp, len);

    // 用户自定义长度
    for (STRING_MAP_ITER it = _headerMap.begin(); it != _headerMap.end(); it++) {
        output->writeBytes(it->first.c_str(), strlen(it->first.c_str()));
        output->writeBytes(": ", 2);
        output->writeBytes(it->second.c_str(), strlen(it->second.c_str()));
        output->writeBytes("\r\n", 2);
    }

    // 空行
    output->writeBytes("\r\n", 2);
    // bodyLen
    output->writeBytes(_body, _bodyLen);
    // assert(_packetHeader._dataLen == output->getDataLen());

    return true;
}

/*
 * 解开
 */
bool HttpResponsePacket::decode(DataBuffer *input, PacketHeader *header) { return true; }

/*
 * 设置header
 */
void HttpResponsePacket::setHeader(const char *name, const char *value) {
    if (name[0] == 'C') {
        if (strcmp(name, "Connection") == 0 || strcmp(name, "Content-Length") == 0) {
            return;
        }
    }
    _headerMap[name] = value;
}

/*
 * 设置状态
 */
void HttpResponsePacket::setStatus(bool status) { _status = status; }

/*
 * 设置内容
 */
void HttpResponsePacket::setBody(const char *body, int len) {
    if (body && (len > 0)) {
        _body = (char *)malloc(len);
        assert(_body);
        memcpy(_body, body, len);
        _bodyLen = len;
    }
}

/*
 * 是否keepalive
 */
void HttpResponsePacket::setKeepAlive(bool keepAlive) { _isKeepAlive = keepAlive; }

} // namespace anet
