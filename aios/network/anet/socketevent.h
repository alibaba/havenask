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
#ifndef ANET_SOCKETEVENT_H_
#define ANET_SOCKETEVENT_H_

namespace anet {

#define MAX_SOCKET_EVENTS 256

class IOComponent;
class Socket;

class IOEvent {

public:
    bool _readOccurred;  // 读发生
    bool _writeOccurred; // 写发生
    bool _errorOccurred; // 错误发生
    IOComponent *_ioc;   // 回传参数
};

class SocketEvent {

public:
    /*
     * 构造函数
     */
    SocketEvent();
    /*
     * 析构函数
     */
    virtual ~SocketEvent();
    /*
     * 增加Socket到事件中
     *
     * @param socket 被加的socket
     * @param enableRead: 设置是否可读
     * @param enableWrite: 设置是否可写
     * @return  操作是否成功, true  成功, false  失败
     */
    virtual bool addEvent(Socket *socket, bool enableRead, bool enableWrite) = 0;

    /*
     * 设置删除Socket到事件中
     *
     * @param socket 被加的socket
     * @param enableRead: 设置是否可读
     * @param enableWrite: 设置是否可写
     * @return  操作是否成功, true  成功, false  失败
     */
    virtual bool setEvent(Socket *socket, bool enableRead, bool enableWrite) = 0;

    /*
     * 删除Socket到事件中
     *
     * @param socket 被删除socket
     * @return  操作是否成功, true  成功, false  失败
     */
    virtual bool removeEvent(Socket *socket) = 0;

    /*
     * 得到读写事件。
     *
     * @param timeout  超时时间(单位:ms)
     * @param events  事件数组
     * @param cnt   events的数组大小
     * @return 事件数, 0为超时
     */
    virtual int getEvents(int timeout, IOEvent *events, int cnt) = 0;

    /**
     * Wake up thread which is blocking in getEvents()
     * This interface may changed in future.
     */
    virtual void wakeUp() = 0;
};
} // namespace anet

#endif /*SOCKETEVENT_H_*/
