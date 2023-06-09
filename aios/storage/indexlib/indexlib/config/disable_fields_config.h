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
#ifndef __INDEXLIB_DISABLE_FIELDS_CONFIG_H
#define __INDEXLIB_DISABLE_FIELDS_CONFIG_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace config {

class DisableFieldsConfig : public autil::legacy::Jsonizable
{
public:
    enum SummaryDisableField { SDF_FIELD_NONE, SDF_FIELD_ALL };
    const static std::string SUMMARY_FIELD_ALL;
    const static std::string SUMMARY_FIELD_NONE;

    enum SourceDisableField { CDF_FIELD_NONE, CDF_FIELD_GROUP, CDF_FIELD_ALL };
    const static std::string SOURCE_FIELD_ALL;
    const static std::string SOURCE_FIELD_NONE;
    const static std::string SOURCE_FIELD_GROUP;

public:
    DisableFieldsConfig();
    ~DisableFieldsConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const DisableFieldsConfig& other) const;

private:
    std::string DisableSummaryToStr(SummaryDisableField summarys) const;
    SummaryDisableField StrToDisableSummary(const std::string& str) const;

    std::string DisableSourceToStr(SourceDisableField summarys) const;
    SourceDisableField StrToDisableSource(const std::string& str);

public:
    std::vector<std::string> attributes;
    std::vector<std::string> indexes;
    std::vector<std::string> packAttributes;
    SummaryDisableField summarys = SDF_FIELD_NONE;
    SourceDisableField sources = CDF_FIELD_NONE;
    std::vector<index::groupid_t> disabledSourceGroupIds;
    bool rewriteLoadConfig = true;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DisableFieldsConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_DISABLE_FIELD_LIST_H
