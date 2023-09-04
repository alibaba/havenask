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
#ifndef ANET_HTTP_REQUEST_PACKET_H
#define ANET_HTTP_REQUEST_PACKET_H
#ifndef _GLIBCXX_PERMIT_BACKWARD_HASH
#define _GLIBCXX_PERMIT_BACKWARD_HASH
#endif
#include <algorithm>
#include <hash_map>

#undef _GLIBCXX_PERMIT_BACKWARD_HASH
#include <string.h>
#include <tr1/unordered_map>

#include "aios/network/anet/packet.h"

namespace anet {
class DataBuffer;

struct eqstr {
    bool operator()(const char *s1, const char *s2) const { return strcmp(s1, s2) == 0; }
};
typedef std::tr1::unordered_map<const char *, const char *, __gnu_cxx::hash<const char *>, eqstr> PSTR_MAP;
typedef PSTR_MAP::iterator PSTR_MAP_ITER;

class HttpRequestPacket : public Packet {
    friend class HttpRequestAndResponsePacketTest_testDecodeAndEncode_Test;

public:
    /*
     * 构造函数
     */
    HttpRequestPacket();

    /*
     * 析构函数
     */
    ~HttpRequestPacket();

    /*
     * 计算出数据包的长度
     */
    void countDataLen();
    /*
     * 组装
     */
    bool encode(DataBuffer *output);

    /*
     * 解开
     */
    bool decode(DataBuffer *input, PacketHeader *header);

    /*
     * 查询串
     */
    char *getQuery();

    /*
     * 是否keepalive
     */
    bool isKeepAlive();

    /*
     * 寻找其他头信息
     */
    const char *findHeader(const char *name);

private:
    char *_strHeader;    // 保存头内容的buffer
    char *_strQuery;     // 查询串
    bool _isKeepAlive;   // 是否支持keepalive
    int _method;         // get - 1
    int _version;        // http version 1-"HTTP/1.0"; 2="HTTP/1.1+"
    PSTR_MAP _headerMap; // 其他头信息的map
};

} // namespace anet

#endif
