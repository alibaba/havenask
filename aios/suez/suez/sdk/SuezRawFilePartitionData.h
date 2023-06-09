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

#include "autil/Lock.h"
#include "suez/sdk/SuezPartitionData.h"

namespace suez {

class SuezRawFilePartitionData final : public SuezPartitionData {
public:
    SuezRawFilePartitionData(const PartitionId &pid);
    ~SuezRawFilePartitionData();

public:
    void setFilePath(const std::string filePath);
    std::string getFilePath() const;

private:
    bool detailEqual(const SuezPartitionData &other) const override;

private:
    mutable autil::ThreadMutex _filePathMutex;
    std::string _filePath;
};

using SuezRawFilePartitionDataPtr = std::shared_ptr<SuezRawFilePartitionData>;
} // namespace suez
