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
#include <iterator>

#include "indexlib/index/aggregation/IValueIterator.h"

namespace indexlibv2::index {

class RandomAccessValueIterator : public IValueIterator
{
public:
    struct StdIteratorWrapper {
        using iterator_category = std::forward_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = autil::StringView;
        using pointer = autil::StringView*;
        using reference = autil::StringView;

        StdIteratorWrapper() = default;
        StdIteratorWrapper(const RandomAccessValueIterator* impl, int idx, bool reverse)
            : impl(impl)
            , idx(idx)
            , reverse(reverse)
        {
        }

        reference operator*() const { return impl->Get(idx); }
        pointer operator->() const
        {
            // should never be called
            assert(false);
            return nullptr;
        }
        StdIteratorWrapper& operator++()
        {
            if (!reverse) {
                ++idx;
            } else {
                --idx;
            }
            return *this;
        }
        StdIteratorWrapper operator++(int)
        {
            auto tmp = *this;
            ++(*this);
            return tmp;
        }

        friend bool operator==(const StdIteratorWrapper& lhs, const StdIteratorWrapper& rhs)
        {
            return lhs.impl == rhs.impl && lhs.idx == rhs.idx;
        }
        friend bool operator!=(const StdIteratorWrapper& lhs, const StdIteratorWrapper& rhs) { return !(lhs == rhs); }

    private:
        const RandomAccessValueIterator* impl = nullptr;
        int idx = 0;
        bool reverse = false;

        friend class RandomAccessValueIterator;
    };

public:
    virtual size_t Size() const = 0;
    virtual autil::StringView Get(size_t idx) const = 0;

public:
    auto begin() const { return StdIteratorWrapper(this, 0, false); }
    auto end() const { return StdIteratorWrapper(this, (int)Size(), false); }
    auto rbegin() const { return StdIteratorWrapper(this, (int)Size() - 1, true); }
    auto rend() const { return StdIteratorWrapper(this, -1, true); }

public:
    static std::unique_ptr<RandomAccessValueIterator> MakeSlice(std::unique_ptr<RandomAccessValueIterator> iter,
                                                                const StdIteratorWrapper& begin,
                                                                const StdIteratorWrapper& end);
};

} // namespace indexlibv2::index
