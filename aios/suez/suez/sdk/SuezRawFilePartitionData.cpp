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
#include "suez/sdk/SuezRawFilePartitionData.h"

using namespace std;

namespace suez {

SuezRawFilePartitionData::SuezRawFilePartitionData(const PartitionId &pid) : SuezPartitionData(pid, SPT_RAWFILE) {}

SuezRawFilePartitionData::~SuezRawFilePartitionData() {}

void SuezRawFilePartitionData::setFilePath(const std::string filePath) {
    autil::ScopedLock lock(_filePathMutex);
    _filePath = filePath;
}

std::string SuezRawFilePartitionData::getFilePath() const {
    autil::ScopedLock lock(_filePathMutex);
    return _filePath;
}

bool SuezRawFilePartitionData::detailEqual(const SuezPartitionData &other) const {
    return getFilePath() == static_cast<const SuezRawFilePartitionData &>(other).getFilePath();
}

} // namespace suez
