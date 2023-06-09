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

class OrcGeneralOptions : public autil::legacy::Jsonizable
{
public:
    OrcGeneralOptions();
    ~OrcGeneralOptions() = default;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    size_t GetRowGroupSize() const { return _rowGroupSize; }
    void SetRowGroupSize(size_t rowGroupSize) { _rowGroupSize = rowGroupSize; }

    size_t GetBuildBufferSize() const { return _buildBufferSize; }
    void SetBuildBufferSize(size_t buildBuffferSize) { _buildBufferSize = buildBuffferSize; }

    bool NeedToJson() const { return !_changedOptions.empty(); }

private:
    size_t _rowGroupSize;
    size_t _buildBufferSize;
    std::set<std::string> _changedOptions;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
