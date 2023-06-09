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

#include <map>
#include <vector>

#include "fmt/core.h"
#include "fmt/format.h"

namespace autil {

struct FormatterWithDefaultParse {
    template <typename ParseContext>
    constexpr auto parse(ParseContext &ctx) {
        return ctx.begin();
    }
};

}

template <typename T>
struct fmt::formatter<std::vector<T>> : autil::FormatterWithDefaultParse {
    template <typename FormatContext>
    auto format(const std::vector<T> &values, FormatContext &ctx) {
        auto &&out = ctx.out();
        for (const auto &value : values) {
            fmt::formatter<T>().format(value, ctx);
        }
        return out;
    }
};

template <typename K, typename V>
struct fmt::formatter<std::pair<K, V>> : autil::FormatterWithDefaultParse {
    template <typename FormatContext>
    auto format(const std::pair<K, V> &value, FormatContext &ctx) {
        auto &&out = ctx.out();
        fmt::formatter<K>().format(value.first, ctx);
        fmt::formatter<char>().format(',', ctx);
        fmt::formatter<V>().format(value.second, ctx);
        return out;
    }
};

template <typename K, typename V>
struct fmt::formatter<std::map<K, V>> : autil::FormatterWithDefaultParse {
    template <typename FormatContext>
    auto format(const std::map<K, V> &value, FormatContext &ctx) {
        auto &&out = ctx.out();
        for (const auto &pair : value) {
            fmt::formatter<K>().format(pair.first, ctx);
            fmt::formatter<char>().format(':', ctx);
            fmt::formatter<V>().format(pair.second, ctx);
        }
        return out;
    }
};
