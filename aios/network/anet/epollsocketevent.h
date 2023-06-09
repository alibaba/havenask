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
#ifndef ANET_EPOLLSOCKETEVENT_H_
#define ANET_EPOLLSOCKETEVENT_H_
#include "aios/network/anet/socketevent.h"

namespace anet {
class Socket;

class EPollSocketEvent : public SocketEvent {

public:
    /*
     * 构造函数
     */
    EPollSocketEvent();

    /*
     * 析造函数
     */
    ~EPollSocketEvent();

    /*
     * 增加Socket到事件中
     *
     * @param socket 被加的socket
     * @param enableRead: 设置是否可读
     * @param enableWrite: 设置是否可写
     * @return  操作是否成功, true  成功, false  失败
     */
    bool addEvent(Socket *socket, bool enableRead, bool enableWrite);

    /*
     * 设置删除Socket到事件中
     *
     * @param socket 被加的socket
     * @param enableRead: 设置是否可读
     * @param enableWrite: 设置是否可写
     * @return  操作是否成功, true  成功, false  失败
     */
    bool setEvent(Socket *socket, bool enableRead, bool enableWrite);

    /*
     * 删除Socket到事件中
     *
     * @param socket 被删除socket
     * @return  操作是否成功, true  成功, false  失败
     */
    bool removeEvent(Socket *socket);

    /*
     * 得到读写事件。
     *
     * @param timeout  超时时间(单位:ms)
     * @param events  事件数组
     * @param cnt   events的数组大小
     * @return 事件数, 0为超时
     */
    int getEvents(int timeout, IOEvent *events, int cnt);

    void wakeUp();

private:
    int _iepfd;    // epoll的fd
    int _pipes[2];  // pipe for wakeup
//    ThreadMutex _mutex;  // 对fd操作加锁
};
}

#endif /*EPOLLSOCKETEVENT_H_*/
