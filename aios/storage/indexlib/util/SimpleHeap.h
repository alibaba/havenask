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

#include <functional>
#include <inttypes.h>
#include <queue>
#include <utility>
#include <vector>

namespace indexlib::util {

template <typename Item, typename Compare>
class SimpleHeap
{
public:
    SimpleHeap(const Compare& comp = Compare()) : _comp(comp) {}

    template <typename Iter>
    void makeHeap(Iter begin, Iter end);

    const Item& top() const { return *_data.begin(); }

    // Attention!! guarantee this item should still on the top after modification.
    Item* mutableTop() { return &_data[0]; }

    template <typename Func>
    void updateTop(Func&& f);

    void pop();

    bool empty() const { return _data.empty(); }
    auto size() const { return _data.size(); }

private:
    Compare _comp;
    std::vector<Item> _data;
};

////////////////////////////////////////////////////////////////////////

template <typename Item, typename Compare>
template <typename Iter>
inline void SimpleHeap<Item, Compare>::makeHeap(Iter begin, Iter end)
{
    _data.assign(begin, end);
    std::make_heap(_data.begin(), _data.end(), _comp);
}

template <typename Item, typename Compare>
template <typename Func>
inline void SimpleHeap<Item, Compare>::updateTop(Func&& f)
{
    std::invoke(std::forward<Func>(f), &(_data[0]));

    const uint64_t end = _data.size();

    uint64_t current = 0;

    while (current < (end - 1) / 2) {
        auto child = 2 * (current + 1); // right child
        if (_comp(_data[child], _data[child - 1])) {
            --child; // left child
        }

        if (_comp(_data[current], _data[child])) {
            std::swap(_data[current], _data[child]);
            current = child;
        } else {
            return;
        }
    }
    if ((end & 1) == 0 && current == (end - 2) / 2) {
        auto child = 2 * current + 1;
        if (_comp(_data[current], _data[child])) {
            std::swap(_data[current], _data[child]);
        }
    }
}

template <typename Item, typename Compare>
inline void SimpleHeap<Item, Compare>::pop()
{
    std::pop_heap(_data.begin(), _data.end(), _comp);
    _data.pop_back();
}

} // namespace indexlib::util
