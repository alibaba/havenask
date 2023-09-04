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
#include "aios/network/anet/epollsocketevent.h"

#include <assert.h>
#include <errno.h>
#include <sys/epoll.h>
#include <unistd.h>

#include "aios/network/anet/filecontrol.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/socket.h"
#include "aios/network/anet/socketevent.h"

namespace anet {
class IOComponent;

/*
 * 构造函数
 */
EPollSocketEvent::EPollSocketEvent() {
    _iepfd = epoll_create(MAX_SOCKET_EVENTS);
    assert(_iepfd >= 0);
    int __attribute__((__unused__)) rc = pipe(_pipes);
    assert(rc == 0);
    FileControl::setCloseOnExec(_iepfd);
    FileControl::setCloseOnExec(_pipes[0]);
    FileControl::setCloseOnExec(_pipes[1]);
    struct epoll_event ev;
    ev.events = EPOLLET;
    ev.data.ptr = this;
    rc = epoll_ctl(_iepfd, EPOLL_CTL_ADD, _pipes[1], &ev);
    assert(rc == 0);
}

/*
 * 析造函数
 */
EPollSocketEvent::~EPollSocketEvent() {
    close(_pipes[0]);
    close(_pipes[1]);
    close(_iepfd);
}

/*
 * 增加Socket到事件中
 *
 * @param socket 被加的socket
 * @param enableRead: 设置是否可读
 * @param enableWrite: 设置是否可写
 * @return  操作是否成功, true  成功, false  失败
 */
bool EPollSocketEvent::addEvent(Socket *socket, bool enableRead, bool enableWrite) {

    struct epoll_event ev;
    ev.data.ptr = socket->getIOComponent();
    // 设置要处理的事件类型
    //    ev.events = EPOLLET;
    ev.events = 0; // use level triggered to reduce call to epoll_ctl()
    if (enableRead) {
        ev.events |= EPOLLIN;
    }
    if (enableWrite) {
        ev.events |= EPOLLOUT;
    }

    //_mutex.lock();
    bool rc = (epoll_ctl(_iepfd, EPOLL_CTL_ADD, socket->getSocketHandle(), &ev) == 0);
    //_mutex.unlock();
    if (!rc)
        ANET_LOG(ERROR, "epoll_ctl error, errno: %d", errno);
    ANET_LOG(DEBUG, "read: %d, write: %d. (IOC:%p)", enableRead, enableWrite, ev.data.ptr);

    return rc;
}

/*
 * 设置删除Socket到事件中
 *
 * @param socket 被加的socket
 * @param enableRead: 设置是否可读
 * @param enableWrite: 设置是否可写
 * @return  操作是否成功, true  成功, false  失败
 */
bool EPollSocketEvent::setEvent(Socket *socket, bool enableRead, bool enableWrite) {

    struct epoll_event ev;
    ev.data.ptr = socket->getIOComponent();
    // 设置要处理的事件类型
    //    ev.events = EPOLLET;
    ev.events = 0; // use level triggered to reduce call to epoll_ctl()
    if (enableRead) {
        ev.events |= EPOLLIN;
    }
    if (enableWrite) {
        ev.events |= EPOLLOUT;
    }

    //_mutex.lock();
    bool rc = (epoll_ctl(_iepfd, EPOLL_CTL_MOD, socket->getSocketHandle(), &ev) == 0);
    //_mutex.unlock();
    // ANET_LOG(ERROR, "EPOLL_CTL_MOD: %d => %d,%d, %d", socket->getSocketHandle(), enableRead, enableWrite,
    // pthread_self());
    return rc;
}

/*
 * 删除Socket到事件中
 *
 * @param socket 被删除socket
 * @return  操作是否成功, true  成功, false  失败
 */
bool EPollSocketEvent::removeEvent(Socket *socket) {

    struct epoll_event ev;
    ev.data.ptr = socket->getIOComponent();
    // 设置要处理的事件类型
    ev.events = 0;
    //_mutex.lock();
    bool rc = (epoll_ctl(_iepfd, EPOLL_CTL_DEL, socket->getSocketHandle(), &ev) == 0);
    //_mutex.unlock();
    // ANET_LOG(ERROR, "EPOLL_CTL_DEL: %d", socket->getSocketHandle());
    return rc;
}

/*
 * 得到读写事件。
 *
 * @param timeout  超时时间(单位:ms)
 * @param events  事件数组
 * @param cnt   events的数组大小
 * @return 事件数, 0为超时, -1为出错了
 */
int EPollSocketEvent::getEvents(int timeout, IOEvent *ioevents, int cnt) {

    struct epoll_event events[MAX_SOCKET_EVENTS];

    if (cnt > MAX_SOCKET_EVENTS) {
        cnt = MAX_SOCKET_EVENTS;
    }

    int res = epoll_wait(_iepfd, events, cnt, timeout);

    // 把events的事件转化成IOEvent的事件
    int j = 0;
    for (int i = 0; i < res; i++) {
        if (events[i].data.ptr == this) {
            continue;
        }

        ioevents[j]._ioc = (IOComponent *)events[i].data.ptr;
        ioevents[j]._errorOccurred = events[i].events & (EPOLLERR | EPOLLHUP) ? true : false;
        ioevents[j]._readOccurred = events[i].events & EPOLLIN ? true : false;
        ioevents[j]._writeOccurred = events[i].events & EPOLLOUT ? true : false;
        j++;
    }

    return j;
}

void EPollSocketEvent::wakeUp() {
    struct epoll_event ev;
    ev.events = EPOLLET | EPOLLOUT;
    ev.data.ptr = this;
    int __attribute__((__unused__)) rc = 0;
    rc = epoll_ctl(_iepfd, EPOLL_CTL_MOD, _pipes[1], &ev);
    assert(rc == 0);
}
} // namespace anet
