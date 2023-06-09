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

#include <forward_list>

#include "autil/CommonMacros.h"

namespace autil::result {

template <typename T, int N>
class ForwardList {
private:
    struct Node {
    public:
        int len{0};
        std::aligned_storage_t<sizeof(T), alignof(T)> arr[N];

    public:
        Node() = default;
        Node(const Node &other) : len{other.len} {
            for (int i = 0; i < len; ++i) {
                new (arr + i) T(other[i]);
            }
        }
        Node(Node &&other) : len{other.len} {
            for (int i = 0; i < len; ++i) {
                new (arr + i) T(std::move(other[i]));
            }
            other.len = 0;
        }
        Node &operator=(const Node &other) {
            if (this == &other)
                return *this;
            this->~Node();
            return *new (this) Node(other);
        }
        Node &operator=(Node &&other) {
            if (this == &other)
                return *this;
            this->~Node();
            return *new (this) Node(std::move(other));
        }
        ~Node() {
            for (int i = 0; i < len; ++i) {
                (*this)[i].~T();
            }
        }

    public:
        template <typename... Args>
        void emplace_back(Args &&...args) {
            assert(len < N);
            new (arr + len++) T(std::forward<Args>(args)...);
        }

        bool full() const { return len == N; }

        const T &operator[](int i) const {
            assert(i < len);
            return *reinterpret_cast<const T *>(arr + i);
        }

        T &operator[](int i) {
            assert(i < len);
            return *reinterpret_cast<T *>(arr + i);
        }
    };

public:
    ForwardList() : _li{1} {}

    template <typename... Args>
    void emplace_back(Args &&...args) {
        if (unlikely(_li.front().full())) {
            _li.emplace_front();
        }
        _li.front().emplace_back(std::forward<Args>(args)...);
    }

    template <typename Lambda>
    void for_each(Lambda &&lambda) const {
        std::vector<const Node *> li;
        for (auto &node : _li) {
            li.emplace_back(&node);
        }
        for (auto it = li.rbegin(); it != li.rend(); ++it) {
            auto &node = *it;
            for (int i = 0; i < node->len; ++i) {
                lambda((*node)[i]);
            }
        }
    }

private:
    std::forward_list<Node> _li;
};

} // namespace autil::result
