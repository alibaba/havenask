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

#include "autil/StringUtil.h"
#include "indexlib/base/Types.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

class DictKeyInfo
{
public:
    DictKeyInfo() : _dictKey(std::numeric_limits<dictkey_t>::max()), _isNull(false) {}

    explicit DictKeyInfo(dictkey_t key, bool isNull = false) : _dictKey(key), _isNull(isNull) {}

    ~DictKeyInfo() {}

public:
    dictkey_t GetKey() const { return _dictKey; }
    void SetKey(dictkey_t key) { _dictKey = key; }

    bool IsNull() const { return _isNull; }
    void SetIsNull(bool flag) { _isNull = flag; }

    std::string ToString() const;
    void FromString(const std::string& str);

public:
    bool operator==(const DictKeyInfo& other) const;
    bool operator!=(const DictKeyInfo& other) const;
    bool operator<(const DictKeyInfo& other) const;
    bool operator>(const DictKeyInfo& other) const;

public:
    static DictKeyInfo NULL_TERM;

private:
    dictkey_t _dictKey;
    bool _isNull;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DictKeyInfo> DickKeyInfoPtr;

////////////////////////////////////////////////////////////
inline bool DictKeyInfo::operator==(const DictKeyInfo& other) const
{
    return _dictKey == other._dictKey && _isNull == other._isNull;
}

inline bool DictKeyInfo::operator!=(const DictKeyInfo& other) const { return !((*this) == other); }

inline bool DictKeyInfo::operator<(const DictKeyInfo& other) const
{
    if (_dictKey != other._dictKey) {
        return _dictKey < other._dictKey;
    }
    return (_isNull != other._isNull) ? !_isNull : false;
}

inline bool DictKeyInfo::operator>(const DictKeyInfo& other) const
{
    if (_dictKey != other._dictKey) {
        return _dictKey > other._dictKey;
    }
    return (_isNull != other._isNull) ? _isNull : false;
}

inline std::string DictKeyInfo::ToString() const
{
    std::string ret = autil::StringUtil::toString(_dictKey);
    if (_isNull) {
        ret += ":true";
    }
    return ret;
}

inline void DictKeyInfo::FromString(const std::string& str)
{
    std::vector<std::string> strVec;
    autil::StringUtil::fromString(str, strVec, ":");

    if (strVec.size() != 1 && strVec.size() != 2) {
        INDEXLIB_FATAL_ERROR(BadParameter, "invalid string [%s] for index::DictKeyInfo", str.c_str());
    }

    if (!autil::StringUtil::fromString(strVec[0], _dictKey)) {
        INDEXLIB_FATAL_ERROR(BadParameter, "invalid string [%s] for index::DictKeyInfo, invalid key", str.c_str());
    }

    if (strVec.size() == 2 && strVec[1] != "true") {
        INDEXLIB_FATAL_ERROR(BadParameter, "invalid string [%s] for index::DictKeyInfo", str.c_str());
    }
    _isNull = (strVec.size() == 2);
}
} // namespace indexlib::index
