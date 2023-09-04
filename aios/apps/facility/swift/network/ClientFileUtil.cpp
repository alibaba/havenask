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
#include "swift/network/ClientFileUtil.h"

#include <fstream>
#include <iterator>
#include <stddef.h>
#include <string>

#include "aios/network/anet/connection.h"
#include "aios/network/anet/httppacket.h"
#include "aios/network/anet/httppacketfactory.h"
#include "aios/network/anet/httpstreamer.h"
#include "aios/network/anet/packet.h"
#include "aios/network/anet/packetqueue.h"
#include "aios/network/anet/transport.h"
#include "zookeeper/zookeeper.h"
#include "zookeeper/zookeeper.jute.h"

using namespace std;
using namespace anet;

namespace swift {
namespace network {
AUTIL_LOG_SETUP(swift, ClientFileUtil);

const static string LOCAL_TYPE = "local";
const static string ZFS_TYPE = "zfs";
const static string HTTP_TYPE = "http";

int ZFS_RECV_TIMEOUT = 6000;
int HTTP_CONNECT_TIMEOUT = 3000;
int HTTP_PACKET_TIMEOUT = 3000;

ClientFileUtil::ClientFileUtil() {}

ClientFileUtil::~ClientFileUtil() {}

bool ClientFileUtil::readFile(const string &fileName, string &content) {
    string type = getFsType(fileName);
    if (type == LOCAL_TYPE) {
        return readLocalFile(fileName, content);
    } else if (type == ZFS_TYPE) {
        return readZfsFile(fileName, content); // avoid use file system
    } else if (type == HTTP_TYPE) {
        return readHttpFile(fileName, content);
    } else {
        return false;
    }
}

string ClientFileUtil::getFsType(const std::string &srcPath) {
    if (srcPath.size() == 0) {
        return "";
    }
    string type;
    size_t found = srcPath.find("://");
    if (found == string::npos) {
        type = LOCAL_TYPE;
    } else {
        type = srcPath.substr(0, found);
    }
    return type;
}

bool ClientFileUtil::readLocalFile(const string &fileName, string &content) {
    std::ifstream fin(fileName);
    if (!fin) {
        AUTIL_LOG(WARN, "open file [%s] failed.", fileName.c_str());
        return false;
    }
    content.append(istreambuf_iterator<char>(fin), istreambuf_iterator<char>());
    return true;
}

#define ZOOKEEPER_CLOSE(zkHandle)                                                                                      \
    {                                                                                                                  \
        int ret = zookeeper_close(zkHandle);                                                                           \
        if (ZOK != ret) {                                                                                              \
            AUTIL_LOG(WARN, "close zk handle [%p] failed, ret [%d]", zkHandle, ret);                                   \
        }                                                                                                              \
    }

bool ClientFileUtil::readZfsFile(const string &fileName, string &content) {
    string server, path;
    if (!parsePath(ZFS_TYPE + "://", fileName, server, path)) {
        AUTIL_LOG(WARN, "parse zfs path [%s] failed.", fileName.c_str());
        return false;
    }
    if (path.size() == 0 || path[0] != '/') {
        AUTIL_LOG(WARN, "path [%s] not match.", path.c_str());
        return false;
    }
    zhandle_t *zkHandle = zookeeper_init(server.c_str(), NULL, ZFS_RECV_TIMEOUT, 0, NULL, 0);
    if (NULL == zkHandle) {
        AUTIL_LOG(WARN, "zookeeper init failed. host: %s", server.c_str());
        return false;
    }
    content.clear();
    char buffer[10240];
    struct Stat stat;
    int buffer_len = sizeof(buffer);
    int ret = zoo_get(zkHandle, path.c_str(), 0, buffer, &buffer_len, &stat);
    if (ZOK != ret) {
        AUTIL_LOG(WARN, "get zfs content fail, file [%s] path[%s], ret [%d].", fileName.c_str(), path.c_str(), ret);
        ZOOKEEPER_CLOSE(zkHandle)
        return false;
    }
    if ((unsigned)stat.dataLength > sizeof(buffer)) {
        char *newBuffer = new char[stat.dataLength];
        buffer_len = stat.dataLength;
        int ret = zoo_get(zkHandle, path.c_str(), 0, newBuffer, &buffer_len, &stat);
        if (ZOK != ret) {
            AUTIL_LOG(WARN, "get zfs content fail, file [%s], ret [%d] ", fileName.c_str(), ret);
            delete[] newBuffer;
            ZOOKEEPER_CLOSE(zkHandle)
            return false;
            // return ZOO_ERRORS(ret);
        }
        content = std::string(newBuffer, (size_t)buffer_len);
        delete[] newBuffer;
    } else if (buffer_len > 0) {
        content.append(buffer, (size_t)buffer_len);
    }
    ZOOKEEPER_CLOSE(zkHandle)
    return true;
}
#undef ZOOKEEPER_CLOSE

bool ClientFileUtil::readHttpFile(const string &fileName, string &content) {
    string server, path;
    if (!parsePath(HTTP_TYPE + "://", fileName, server, path)) {
        AUTIL_LOG(WARN, "parse http path [%s] failed.", fileName.c_str());
        return false;
    }
    anet::Transport transport;
    if (!transport.start()) {
        AUTIL_LOG(WARN, "start transport failed.");
        return false;
    }
    static anet::HTTPPacketFactory packetFactory;
    static anet::HTTPStreamer packetStreamer(&packetFactory);
    string tcpServer = "tcp://" + server;
    Connection *connection = transport.connect(tcpServer.c_str(), &packetStreamer);
    if (!connection) {
        AUTIL_LOG(ERROR, "connect to %s failed", server.c_str());
        transport.stop();
        transport.wait();
        return false;
    }
    connection->setQueueLimit(10);
    connection->setQueueTimeout(HTTP_CONNECT_TIMEOUT);
    AUTIL_LOG(INFO, "create http anet connection to [%s] success", server.c_str());

    anet::HTTPPacket *packet = new anet::HTTPPacket();
    packet->setMethod(anet::HTTPPacket::HM_GET);
    packet->setURI(path.c_str());
    packet->addHeader("Connection", "close");
    packet->setExpireTime(HTTP_PACKET_TIMEOUT);
    Packet *replyPacket = connection->sendPacket(packet);
    if (replyPacket == NULL) {
        AUTIL_LOG(ERROR, "post packet failed, [%s]", server.c_str());
        packet->free();
        transport.stop();
        transport.wait();
        connection->subRef();
        return false;
    }
    anet::HTTPPacket *httpPacket = dynamic_cast<HTTPPacket *>(replyPacket);
    if (httpPacket == NULL) {
        AUTIL_LOG(ERROR, "dynamic cast http packet failed.");
        replyPacket->free();
        transport.stop();
        transport.wait();
        connection->subRef();
        return false;
    }
    size_t dataLen = 0;
    const char *data = httpPacket->getBody(dataLen);
    content.append(data, dataLen);
    replyPacket->free();
    transport.stop();
    transport.wait();
    connection->subRef();
    return true;
}

bool ClientFileUtil::parsePath(const string &type, const string &fileName, string &server, string &path) {
    size_t posBeg = fileName.find(type);
    if (posBeg != 0) {
        AUTIL_LOG(INFO, "parse path [%s] type [%s] failed", fileName.c_str(), type.c_str());
        return false;
    }
    posBeg += type.size();
    size_t posEnd = fileName.find('/', posBeg);
    if (posEnd == string::npos) {
        AUTIL_LOG(INFO, "can find / in path [%s]. ", fileName.c_str());
        return false;
    }
    server = fileName.substr(posBeg, posEnd - posBeg);
    path = fileName.substr(posEnd);
    return true;
}

} // namespace network
} // namespace swift
