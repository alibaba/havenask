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
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/PathUtil.h"

namespace indexlib { namespace file_system {

class LifecycleTable : public autil::legacy::Jsonizable
{
public:
    LifecycleTable() {}
    ~LifecycleTable() {}

public:
    using SegmentDirMapType = std::map<std::string, std::string>;
    static const std::string SEGMENT_DIR_MAP;

public:
    bool AddDirectory(const std::string& path, const std::string& lifecycle)
    {
        return InnerAdd(Normalize(path, true), lifecycle);
    }

    void RemoveDirectory(const std::string& path)
    {
        auto normPath = Normalize(path, true);
        _lifecycleMap.erase(normPath);
    }

    void RemoveFile(const std::string& path)
    {
        auto normPath = Normalize(path, false);
        _lifecycleMap.erase(normPath);
    }

    // dir path should be ended with '/'
    std::string GetLifecycle(const std::string& path) const;

    bool IsEmpty() const;

    size_t Size() const { return _lifecycleMap.size(); }

    void Jsonize(JsonWrapper& json) override;

    SegmentDirMapType::const_iterator Begin() const { return _lifecycleMap.begin(); }
    SegmentDirMapType::const_iterator End() const { return _lifecycleMap.end(); }

    bool operator==(const LifecycleTable& other) const;

private:
    bool InnerAdd(const std::string& path, const std::string& lifecycle);
    std::string Normalize(const std::string& path, bool isDirectory) const
    {
        auto normPath = util::PathUtil::NormalizePath(path);
        if (isDirectory) {
            normPath += "/";
        }
        return normPath == "/" ? "" : normPath;
    }

private:
    SegmentDirMapType _lifecycleMap;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LifecycleTable> LifecycleTablePtr;
}} // namespace indexlib::file_system
