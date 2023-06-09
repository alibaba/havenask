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

#include "autil/DataBuffer.h"


namespace isearch {
namespace common {

struct QrsSearchInfo
{
public:
    QrsSearchInfo() {}
    ~QrsSearchInfo() {}

public:
    const std::string& getQrsSearchInfo() const {
        return qrsLogInfo;
    }
    void appendQrsInfo(const std::string& info) {
        qrsLogInfo.append(info);
    }
    void reset() {
        qrsLogInfo.clear();
    }
    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(qrsLogInfo);
    }
    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(qrsLogInfo);
    }
    bool operator == (const QrsSearchInfo &other) const {
        return qrsLogInfo == other.qrsLogInfo;
    }
public:
    std::string qrsLogInfo;
};

typedef std::shared_ptr<QrsSearchInfo> QrsSearchInfoPtr;

} // namespace common
} // namespace isearch

