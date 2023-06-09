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
#ifndef ISEARCH_BS_DATADESCRIPTIONS_H
#define ISEARCH_BS_DATADESCRIPTIONS_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/util/Log.h"

namespace build_service { namespace proto {

class DataDescriptions
{
public:
    DataDescriptions();
    ~DataDescriptions();

public:
    DataDescription& operator[](size_t n)
    {
        assert(n < _dataDescriptions.size());
        return _dataDescriptions[n];
    }
    const DataDescription& operator[](size_t n) const
    {
        assert(n < _dataDescriptions.size());
        return _dataDescriptions[n];
    }
    void push_back(const DataDescription& x) { _dataDescriptions.push_back(x); }
    void pop_back() { _dataDescriptions.pop_back(); }
    bool empty() const { return _dataDescriptions.empty(); }
    size_t size() const { return _dataDescriptions.size(); }
    bool operator==(const DataDescriptions& other) const { return toVector() == other.toVector(); }
    void clear() { _dataDescriptions.clear(); }

public:
    // for jsonize, autil Jsonizable not support jsonize array on root
    const std::vector<DataDescription>& toVector() const { return _dataDescriptions; }
    std::vector<DataDescription>& toVector() { return _dataDescriptions; }

public:
    bool validate() const;

private:
    std::vector<DataDescription> _dataDescriptions;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::proto

#endif // ISEARCH_BS_DATADESCRIPTIONS_H
