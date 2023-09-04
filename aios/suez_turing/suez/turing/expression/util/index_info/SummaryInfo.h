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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace suez {
namespace turing {

class SummaryInfo : public autil::legacy::Jsonizable {
public:
    typedef std::vector<std::string> StringVector;
    typedef std::map<std::string, size_t> Name2PosMap;

public:
    SummaryInfo();
    ~SummaryInfo();

public:
    size_t getFieldCount() const;
    std::string getFieldName(uint32_t pos) const;
    void addFieldName(const std::string &fieldName);
    bool exist(const std::string &fieldName) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json);
    void stealSummaryInfos(StringVector &summaryInfos);

private:
    StringVector _fieldNameVector;
    Name2PosMap _name2PosMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
