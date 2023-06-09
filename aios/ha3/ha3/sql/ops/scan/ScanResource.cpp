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
#include "ha3/sql/ops/scan/ScanResource.h"

#include "indexlib/partition/partition_reader_snapshot.h"

using namespace std;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, ScanResource);

ScanResource::ScanResource() {
    range.first = 0;
    range.second = autil::RangeUtil::MAX_PARTITION_RANGE;
}

ScanResource::~ScanResource() {}

std::shared_ptr<indexlib::partition::IndexPartitionReader>
ScanResource::getPartitionReader(const std::string &tableName) const {
    if (!partitionReaderSnapshot) {
        return nullptr;
    }
    return partitionReaderSnapshot->GetIndexPartitionReader(tableName);
}

std::shared_ptr<indexlibv2::framework::ITabletReader>
ScanResource::getTabletReader(const std::string &tableName) const {
    if (!partitionReaderSnapshot) {
        return nullptr;
    }
    return partitionReaderSnapshot->GetTabletReader(tableName);
}

bool ScanResource::isLeader(const std::string &tableName) const {
    if (!partitionReaderSnapshot) {
        return false;
    }
    return partitionReaderSnapshot->isLeader(tableName);
}

int64_t ScanResource::getBuildOffset(const std::string &tableName) const {
    if (!partitionReaderSnapshot) {
        return -1;
    }
    return partitionReaderSnapshot->getBuildOffset(tableName);

}

} // namespace sql
} // namespace isearch
