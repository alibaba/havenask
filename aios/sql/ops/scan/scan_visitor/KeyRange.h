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

#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "sql/common/Log.h"

namespace sql {

class KeyRangeBase {
public:
    virtual ~KeyRangeBase() {}

public:
    virtual void insertAndTrim(KeyRangeBase *others) = 0;
    virtual void intersectionRanges(KeyRangeBase *others) = 0;
    virtual std::shared_ptr<indexlib::table::DimensionDescription> convertDimenDescription() = 0;
};

typedef std::shared_ptr<KeyRangeBase> KeyRangeBasePtr;

template <typename T>
class KeyRangeTyped : public KeyRangeBase {
    static const int DISCRETE_POINT_LIMIT = 10;

public:
    KeyRangeTyped(const std::string &attrName)
        : _attrName(attrName) {}

private:
    static bool compare(const std::pair<T, T> &a, const std::pair<T, T> &b) {
        return a.first == b.first ? a.second < b.second : a.first < b.first;
    }

public:
    void add(const T &start, const T &end) {
        if (end >= start) {
            _ranges.push_back(std::make_pair(start, end));
        }
    }

    void insertAndTrim(KeyRangeBase *others) override {
        auto otherRanges = dynamic_cast<KeyRangeTyped<T> *>(others);
        if (otherRanges == nullptr) {
            return;
        }
        _ranges.insert(_ranges.end(), otherRanges->_ranges.begin(), otherRanges->_ranges.end());
        trimRange();
    }

    void intersectionRanges(KeyRangeBase *others) override {
        auto rhs = dynamic_cast<KeyRangeTyped<T> *>(others);
        if (rhs == nullptr) {
            return;
        }
        if (_attrName != rhs->_attrName) {
            return;
        }
        std::vector<std::pair<T, T>> uni;
        std::swap(uni, _ranges);
        uni.insert(uni.begin(), rhs->_ranges.begin(), rhs->_ranges.end());
        if (uni.empty()) {
            return;
        }
        std::sort(uni.begin(), uni.end(), compare);
        T y = uni[0].second;
        for (size_t idx = 1; idx < uni.size(); ++idx) {
            const auto &curRange = uni[idx];
            if (y < curRange.first) {
                y = curRange.second;
            } else {
                if (y < curRange.second) {
                    _ranges.push_back(std::make_pair(curRange.first, y));
                    y = curRange.second;
                } else {
                    _ranges.push_back(std::make_pair(curRange.first, curRange.second));
                }
            }
        }
        return;
    }
    std::shared_ptr<indexlib::table::DimensionDescription> convertDimenDescription() override {
        std::shared_ptr<indexlib::table::DimensionDescription> dimen(
            new indexlib::table::DimensionDescription);
        dimen->name = _attrName;
        for (const auto &range : _ranges) {
            const T &begin = range.first;
            const T &end = range.second;
            assert(end >= begin);
            if (end < begin) {
                SQL_LOG(ERROR, "unexpected, end[%ld] > begin[%ld]", int64_t(end), int64_t(begin));
                return {};
            }
            if (end < begin + DISCRETE_POINT_LIMIT) {
                for (T i = begin; i <= end; ++i) {
                    dimen->values.emplace_back(autil::StringUtil::toString(i));
                }
            } else {
                dimen->ranges.push_back(
                    {autil::StringUtil::toString(begin), autil::StringUtil::toString(end)});
            }
        }
        return dimen;
    }

private:
    void trimRange() {
        if (_ranges.empty()) {
            return;
        }
        std::sort(_ranges.begin(), _ranges.end(), compare);
        std::vector<std::pair<T, T>> trimedRange;
        T x = _ranges[0].first;
        T y = _ranges[0].second;
        for (size_t idx = 1; idx < _ranges.size(); ++idx) {
            if (y < _ranges[idx].first) {
                trimedRange.push_back(std::make_pair(x, y));
                x = _ranges[idx].first;
                y = _ranges[idx].second;
            } else {
                y = std::max(y, _ranges[idx].second);
            }
        }
        trimedRange.push_back(std::make_pair(x, y));
        _ranges = std::move(trimedRange);
    }

private:
    std::vector<std::pair<T, T>> _ranges;
    std::string _attrName;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(sql, KeyRangeTyped, T);

} // namespace sql
