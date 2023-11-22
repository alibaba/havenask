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

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/source_group_config.h"
#include "indexlib/config/summary_group_config.h"
#include "indexlib/index/data_structure/var_len_data_param.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class VarLenDataParamHelper
{
public:
    VarLenDataParamHelper();
    ~VarLenDataParamHelper();

public:
    static VarLenDataParam MakeParamForAttribute(const config::AttributeConfigPtr& attrConfig);

    static VarLenDataParam MakeParamForSummary(const config::SummaryGroupConfigPtr& summaryGroupConfig);

    static VarLenDataParam MakeParamForSourceData(const config::SourceGroupConfigPtr& sourceGroupConfig);

    static VarLenDataParam MakeParamForSourceMeta();

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarLenDataParamHelper);
}} // namespace indexlib::index
