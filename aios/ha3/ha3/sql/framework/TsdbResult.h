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
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#ifndef AIOS_OPEN_SOURCE
#include "khronos/table_interface/CommonDefine.h"
#endif

namespace isearch {
namespace sql {

class TsdbResult : public autil::legacy::Jsonizable {
public:
    TsdbResult()
        : summary(-1.0)
        , messageWatermark(-1.0)
        , integrity(-1.0) {}
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("metric", metric);
        json.Jsonize("summary", summary);
        json.Jsonize("messageWatermark", messageWatermark);
        json.Jsonize("integrity", integrity);
        json.Jsonize("tags", tags);
        json.Jsonize("dps", dps);
        json.Jsonize("strdps", strdps);
    }

public:
    std::string metric;
    double summary;
#ifndef AIOS_OPEN_SOURCE
    khronos::KHR_WM_TYPE messageWatermark;
#else
    int64_t messageWatermark;
#endif
    float integrity;
    std::map<std::string, std::string> tags;
    std::map<std::string, float> dps;
    std::map<std::string, std::string> strdps;
};

typedef std::vector<TsdbResult> TsdbResults;

typedef std::shared_ptr<TsdbResult> TsdbResultPtr;
} // namespace sql
} // namespace isearch
