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
#ifndef _CRON_FREELIST_H_
#define _CRON_FREELIST_H_

#include <new>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

namespace cm_basic {

/**
 * @brief 延迟释放对象链表结点。
 **/
template <typename T>
struct FreeNode {
    FreeNode(T* p, int64_t ts) : pointer(p), next(NULL), timestamp(ts) {}

    ~FreeNode() {}

    T* pointer;
    FreeNode* next;
    int64_t timestamp;
};

/**
 * @brief 延迟释放对象链表。非线程安全
 **/
template <typename T>
class CronFreeList
{
public:
    /**
     * @brief 构造函数
     * @param seconds 延迟释放时间，单位为秒。默认10s后释放
     **/
    CronFreeList(int seconds = 10) : _head(NULL), _tail(NULL), _seconds(seconds) {}

    /**
     * @brief 析构函数。析构时释放当前链表内保存的所有对象。
     **/
    ~CronFreeList()
    {
        FreeNode<T>* node = NULL;
        FreeNode<T>* next = NULL;

        node = _head;
        while (node != NULL) {
            next = node->next;
            delete node->pointer;
            delete node;
            node = next;
        }
    }

    /**
     * @brief 增加一个待释放的对象。插入该对象前，先检查并释放所有已到期/过期的对象
     *
     * @param ptr 待释放对象指针
     **/
    bool pushBack(T* ptr, int64_t ts = 0)
    {
        if (ptr == NULL) {
            return false;
        }

        if (ts == 0) {
            ts = time(NULL);
        }
        freeFront(ts);

        FreeNode<T>* node = new (std::nothrow) FreeNode<T>(ptr, ts);
        assert(node != NULL);

        if (_head == NULL) {
            _head = node;
            _tail = node;
        } else {
            _tail->next = node;
            _tail = node;
        }

        return true;
    }

private:
    /**
     * @brief 检查并释放所有已到期/过期的对象，并将其从链表里删除
     *
     * @param ts 当前时间
     **/
    void freeFront(int64_t ts)
    {
        FreeNode<T>* node = NULL;
        FreeNode<T>* next = NULL;

        node = _head;
        while (node != NULL) {
            next = node->next;
            if (node->timestamp + _seconds > ts) {
                break;
            }

            delete node->pointer;
            delete node;
            node = next;
        }

        _head = node;
        if (_head == NULL) {
            _tail = NULL;
        }
    }

private:
    FreeNode<T>* _head;
    FreeNode<T>* _tail;
    int _seconds;
};

} // namespace cm_basic

#endif /* LIBDETAIL_CRON_FREE_LIST_H */
