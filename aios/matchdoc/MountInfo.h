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
#include <memory>
#include <string>
#include <type_traits>

#include "autil/MultiValueType.h"
#include "matchdoc/CommonDefine.h"

namespace matchdoc {

template <typename T>
struct IsMountable {
    // maybe derive this value from indexlib
    static constexpr bool value = std::is_arithmetic<T>::value || autil::IsMultiType<T>::value;
};

struct MountMeta {
    uint32_t mountId;
    uint64_t mountOffset;
    MountMeta() : mountId(INVALID_MOUNT_ID), mountOffset(INVALID_OFFSET) {}

    MountMeta(uint32_t id, uint64_t offset) : mountId(id), mountOffset(offset) {}
};

class MountInfo {
public:
    MountInfo() {}
    ~MountInfo() {}
    MountInfo(const MountInfo &other) : _mountMap(other._mountMap) {}

private:
    typedef std::map<std::string, MountMeta> MountMap;

public:
    // variables with same mountId will share the same baseAddress
    bool insert(const std::string &fieldName, uint32_t mountId, uint64_t mountOffset) {
        return _mountMap.insert(std::make_pair(fieldName, MountMeta(mountId, mountOffset))).second;
    }

    const MountMeta *get(const std::string &fieldName) const {
        MountMap::const_iterator it = _mountMap.find(fieldName);
        if (it == _mountMap.end()) {
            return nullptr;
        }
        return &(it->second);
    }

    bool erase(const std::string &fieldName) {
        auto it = _mountMap.find(fieldName);
        if (it == _mountMap.end()) {
            return false;
        }
        _mountMap.erase(it);
        return true;
    }

    const MountMap &getMountMap() const { return _mountMap; }

public:
    static std::string generateMountGroupName(uint32_t mountId) {
        static const std::string PREFIX = "__mounted_";
        return PREFIX + std::to_string(mountId);
    }

private:
    MountMap _mountMap;
};

typedef std::shared_ptr<MountInfo> MountInfoPtr;

} // namespace matchdoc
