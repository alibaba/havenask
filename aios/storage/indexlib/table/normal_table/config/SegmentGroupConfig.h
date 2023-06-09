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
#include "autil/legacy/jsonizable.h"

namespace indexlibv2::table {

class SegmentGroupConfig : public autil::legacy::Jsonizable
{
public:
    SegmentGroupConfig();
    SegmentGroupConfig(const SegmentGroupConfig& other);
    ~SegmentGroupConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    std::vector<std::string> GetGroups() const;
    std::vector<std::string> GetFunctionNames() const;
    bool IsGroupEnabled() const;

public:
    void TEST_SetGroups(std::vector<std::string> groups);

public:
    //如果配置了group并且非空，则会默认在最后加一个default_group，
    //并且对应的function会一定返回true，来保证每一个doc都能有对应的group
    inline static const std::string DEFAULT_GROUP = "default_group";

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace indexlibv2::table
