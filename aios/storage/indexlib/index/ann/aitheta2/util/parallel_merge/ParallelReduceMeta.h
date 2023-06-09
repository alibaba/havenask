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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/Directory.h"

namespace indexlibv2::index::ann {

class ParallelReduceMeta : public autil::legacy::Jsonizable
{
public:
    ParallelReduceMeta(uint32_t count) : parallelCount(count) {}
    ParallelReduceMeta() : parallelCount(0) {}

public:
    static std::string GetFileName() { return FILE_NAME; }
    std::string GetInsDirName(uint32_t id);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize(PARALLEL_COUNT_KEY, parallelCount, parallelCount);
    }

    void Store(const indexlib::file_system::DirectoryPtr& directory) const;
    bool Load(const indexlib::file_system::DirectoryPtr& directory);

public:
    uint32_t parallelCount;

    inline static const std::string PARALLEL_COUNT_KEY = "parallel_count";
    inline static const std::string FILE_NAME = "parallel_meta";
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index::ann