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

#include "autil/legacy/jsonizable.h"
#include "indexlib/util/RegularExpression.h"
#include "indexlib/util/Singleton.h"

namespace indexlib { namespace util {

class IndexlibMetricControl : public util::Singleton<IndexlibMetricControl>
{
public:
    struct PatternItem : public autil::legacy::Jsonizable {
    public:
        PatternItem() : prohibit(false) {}

    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("pattern", patternStr, patternStr);
            json.Jsonize("id", gatherId, gatherId);
            json.Jsonize("prohibit", prohibit, prohibit);
        }

    public:
        std::string patternStr;
        std::string gatherId;
        bool prohibit;
    };

public:
    struct Status {
    public:
        Status(std::string _gatherId = "", bool _prohibit = false) : gatherId(_gatherId), prohibit(_prohibit) {}

    public:
        std::string gatherId;
        bool prohibit;
    };

public:
    IndexlibMetricControl();
    ~IndexlibMetricControl();

public:
    void InitFromString(const std::string& paramStr);
    Status Get(const std::string& metricName) const noexcept;

public:
    void TEST_RESET() { _statusItems.clear(); }

private:
    void InitFromEnv();
    void ParsePatternItem(std::vector<PatternItem>& patternItems, const std::string& patternStr);
    void ParseFlatString(std::vector<PatternItem>& patternItems, const std::string& patternStr);
    void ParseJsonString(std::vector<PatternItem>& patternItems, const std::string& patternStr);

    bool IsExist(const std::string& filePath);
    void AtomicLoad(const std::string& filePath, std::string& fileContent);

private:
    typedef std::pair<util::RegularExpressionPtr, Status> StatusItem;
    typedef std::vector<StatusItem> StatusItemVector;

    StatusItemVector _statusItems;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexlibMetricControl> IndexlibMetricControlPtr;
}} // namespace indexlib::util
