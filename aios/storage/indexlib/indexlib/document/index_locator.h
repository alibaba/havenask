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

#include <memory>

#include "indexlib/base/Progress.h"
#include "indexlib/common_define.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

// move from build_service
class IndexLocator
{
public:
    explicit IndexLocator(int64_t offset = -1) : _src(0), _offset(offset)
    {
        _progress.emplace_back(indexlibv2::base::Progress(_offset));
    }

    IndexLocator(uint64_t src, int64_t offset, const std::string& userData)
        : _src(src)
        , _offset(offset)
        , _userData(userData)
    {
        _progress.emplace_back(indexlibv2::base::Progress(_offset));
    }

    IndexLocator(const IndexLocator& other)
        : _src(other._src)
        , _offset(other._offset)
        , _userData(other._userData)
        , _progress(other._progress)
    {
    }

    ~IndexLocator() {}

public:
    indexlibv2::framework::Locator ToLocator() const
    {
        indexlibv2::framework::Locator locator(getSrc(), getOffset());
        locator.SetUserData(getUserData());
        locator.SetProgress(getProgress());
        return locator;
    }
    friend bool operator==(const IndexLocator& lft, const IndexLocator& rht)
    {
        return lft._offset == rht._offset and lft._src == rht._src and lft._userData == rht._userData;
    }
    friend bool operator!=(const IndexLocator& lft, const IndexLocator& rht) { return !(lft == rht); }

    uint64_t getSrc() const { return _src; }
    void setSrc(uint64_t src) { _src = src; }
    int64_t getOffset() const { return _offset; }
    void setOffset(int64_t offset)
    {
        _offset = offset;
        _progress.clear();
        _progress.emplace_back(indexlibv2::base::Progress(_offset));
    }
    const std::string& getUserData() const { return _userData; }
    void setUserData(const std::string& userData) { _userData = userData; }

    void setProgress(const std::vector<indexlibv2::base::Progress>& progress)
    {
        if (progress.empty()) {
            return;
        }
        _progress = progress;
        _offset = progress[0].offset;
        for (const auto& singleProgress : progress) {
            _offset = std::min(_offset, singleProgress.offset);
        }
    }
    const std::vector<indexlibv2::base::Progress>& getProgress() const { return _progress; }
    // WARNING: only use this to serialize/deserialize
    std::string toString() const;

    template <typename StringType>
    bool fromString(const StringType& str)
    {
        if (str.size() < sizeof(_offset) + sizeof(_src)) {
            return false;
        }
        char* data = (char*)str.data();
        _src = *(uint64_t*)data;
        data += sizeof(_src);
        _offset = *(int64_t*)data;
        data += sizeof(_offset);
        _userData = std::string(data, str.size() - sizeof(_offset) - sizeof(_src));
        _progress.clear();
        _progress.emplace_back(indexlibv2::base::Progress(_offset));
        return true;
    }

    // use this to LOG
    std::string toDebugString() const;

    size_t size() const { return sizeof(_src) + sizeof(_offset) + _userData.size(); }

private:
    uint64_t _src;
    int64_t _offset;
    // In case the data source does not provide int64_t type offset, use _userData to keep track of the real offset and
    // _offset can be set to an arbitrary monotonic increasing number, e.g.  a counter.
    std::string _userData;
    std::vector<indexlibv2::base::Progress> _progress;
};

class SrcSignature
{
public:
    SrcSignature() : _src(0), _valid(false) {}
    SrcSignature(uint64_t src) : _src(src), _valid(true) {}

    bool Get(uint64_t* src) const
    {
        if (_valid) {
            *src = _src;
        }
        return _valid;
    }

private:
    uint64_t _src;
    bool _valid;
};

extern const IndexLocator INVALID_INDEX_LOCATOR;
}} // namespace indexlib::document
