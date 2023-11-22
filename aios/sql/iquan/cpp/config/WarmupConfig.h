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

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"

namespace iquan {

class WarmupConfig : public autil::legacy::Jsonizable {
public:
    WarmupConfig()
        : threadNum(3)
        , warmupSeconds(30)
        , warmupQueryNum(1000000)
        , warmupLogName("sql_warmup") {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override {
        json.Jsonize("thread_number", threadNum, threadNum);
        json.Jsonize("warmup_second_interval", warmupSeconds, warmupSeconds);
        json.Jsonize("warmup_query_number", warmupQueryNum, warmupQueryNum);
        json.Jsonize("warmup_file_path", warmupFilePath, warmupFilePath);
        json.Jsonize("warmup_log_name", warmupLogName, warmupLogName);
    }

    bool isValid() const {
        if (threadNum <= 0 || warmupSeconds <= 0 || warmupQueryNum <= 0) {
            return false;
        }
        return true;
    }

public:
    int64_t threadNum;
    int64_t warmupSeconds;
    int64_t warmupQueryNum;
    std::string warmupFilePath;
    std::string warmupLogName;
    mutable std::vector<std::string> warmupFilePathList;
};

} // namespace iquan
