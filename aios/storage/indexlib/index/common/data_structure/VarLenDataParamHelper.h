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

namespace indexlib::config {
class SourceGroupConfig;
} // namespace indexlib::config

namespace indexlibv2::config {
class SummaryGroupConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {
struct VarLenDataParam;
class AttributeConfig;

class VarLenDataParamHelper
{
public:
    VarLenDataParamHelper();
    ~VarLenDataParamHelper();

public:
    static VarLenDataParam MakeParamForAttribute(const std::shared_ptr<index::AttributeConfig>& attrConfig);

    static VarLenDataParam
    MakeParamForSummary(const std::shared_ptr<indexlibv2::config::SummaryGroupConfig>& summaryGroupConfig);

    static VarLenDataParam
    MakeParamForSourceData(const std::shared_ptr<indexlib::config::SourceGroupConfig>& sourceGroupConfig);

    static VarLenDataParam MakeParamForSourceMeta();

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
