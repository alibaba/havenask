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

namespace suez {

class Checkpoint : public autil::legacy::Jsonizable {
public:
    Checkpoint();
    ~Checkpoint();

public:
    void updatePersistedLogId(int64_t id);
    int64_t getPersistedLogId() const { return _persistedLogId; }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
    std::string toString() const;
    bool fromString(const std::string &content);

public:
    static std::string getCheckpointFileName(const std::string &root);

private:
    int64_t _persistedLogId = -1;
    std::string _generator;
};

} // namespace suez
