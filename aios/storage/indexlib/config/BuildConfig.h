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

namespace indexlibv2::config {

class BuildConfig : public autil::legacy::Jsonizable
{
public:
    BuildConfig();
    BuildConfig(const BuildConfig& other);
    BuildConfig& operator=(const BuildConfig& other);
    ~BuildConfig();

    void InitForOnline();
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

public:
    int64_t GetBuildingMemoryLimit() const;
    uint64_t GetMaxDocCount() const;
    uint32_t GetKeepVersionCount() const;
    uint32_t GetKeepVersionHour() const;
    int64_t GetFenceTsTolerantDeviation() const;
    int32_t GetMaxCommitRetryCount() const;
    bool GetIsEnablePackageFile() const;
    uint32_t GetDumpThreadCount() const;

public:
    void TEST_init();
    void TEST_SetBuildingMemoryLimit(int64_t limit);
    void TEST_SetMaxDocCount(uint64_t count);
    void TEST_SetKeepVersionCount(uint32_t count);
    void TEST_SetFenceTsTolerantDeviation(int64_t tolerantDeviation);
    void TEST_SetEnablePackageFile(bool enable);
    void TEST_SetDumpThreadCount(uint32_t count);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
