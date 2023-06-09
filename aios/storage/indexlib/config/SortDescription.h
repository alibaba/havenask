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

namespace indexlibv2 { namespace config {

enum SortPattern { sp_nosort, sp_asc, sp_desc };

class SortDescription : public autil::legacy::Jsonizable
{
public:
    SortDescription();
    ~SortDescription();
    SortDescription(const std::string& sortFieldName, SortPattern sortPattern);
    SortDescription(const SortDescription& other);
    SortDescription& operator=(const SortDescription& other);

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const SortDescription& other) const;
    bool operator!=(const SortDescription& other) const;
    std::string ToString() const;

    static SortPattern SortPatternFromString(const std::string& sortPatternStr);
    static std::string SortPatternToString(const SortPattern& sortPattern);

public:
    const std::string& GetSortFieldName() const;
    const SortPattern& GetSortPattern() const;

public:
    inline const static std::string DESC_SORT_PATTERN = "DESC";
    inline const static std::string ASC_SORT_PATTERN = "ASC";

public:
    void TEST_SetSortFieldName(const std::string& fieldName);
    void TEST_SetSortPattern(const SortPattern& sortPattern);
    // format: +fieldName;-fieldName
    static std::vector<SortDescription> TEST_Create(const std::string& str);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};
typedef std::vector<SortDescription> SortDescriptions;

}} // namespace indexlibv2::config
