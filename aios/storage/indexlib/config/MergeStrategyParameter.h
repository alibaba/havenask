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
#include <string>

#include "autil/legacy/jsonizable.h"

namespace indexlibv2::config {

class MergeStrategyParameter : public autil::legacy::Jsonizable
{
public:
    MergeStrategyParameter();
    MergeStrategyParameter(const MergeStrategyParameter& other);
    MergeStrategyParameter& operator=(const MergeStrategyParameter& other);
    ~MergeStrategyParameter();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

public:
    // for legacy format compatible
    void SetLegacyString(const std::string& legacyStr);
    std::string GetLegacyString() const;
    std::string GetStrategyConditions() const;

public:
    static MergeStrategyParameter CreateFromLegacyString(const std::string& paramStr);
    static MergeStrategyParameter Create(const std::string& inputLimit, const std::string& strategyConditions,
                                         const std::string& outputLimit);

public:
    struct Impl {
        std::string inputLimitParam;
        std::string strategyConditions;
        std::string outputLimitParam;
    };
    std::unique_ptr<Impl> _impl;
};

} // namespace indexlibv2::config
