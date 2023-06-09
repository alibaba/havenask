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

#include <future>

#include "suez/table/wal/WALStrategy.h"

namespace suez {

class NoneWAL : public WALStrategy {
public:
    void log(const std::vector<std::pair<uint16_t, std::string>> &str, CallbackType done) override {
        std::vector<int64_t> ts;
        for (size_t i = 0; i < str.size(); i++) {
            ts.push_back(counter++);
        }
        cbs.push_back(std::async(std::launch::async, done, ts));
    }
    void stop() override{};
    void flush() override{};

    void waitDone() {
        for (auto &cb : cbs) {
            cb.get();
        }
        cbs.clear();
    }

    int64_t counter = 10;
    std::vector<std::future<void>> cbs;
};

} // namespace suez
