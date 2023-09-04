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
#include "suez/table/SuezPartitionFactory.h"

#include <iosfwd>

#include "autil/Log.h"
#include "suez/table/SuezIndexPartition.h"
#include "suez/table/SuezRawFilePartition.h"
#include "suez/table/SuezTabletPartition.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SuezPartitionFactory);

namespace suez {

SuezPartitionFactory::SuezPartitionFactory() {}

SuezPartitionFactory::~SuezPartitionFactory() {}

SuezPartitionPtr SuezPartitionFactory::create(SuezPartitionType type,
                                              const TableResource &tableResource,
                                              const CurrentPartitionMetaPtr &partitionMeta) {
    switch (type) {
    case SPT_RAWFILE:
        return SuezPartitionPtr(new SuezRawFilePartition(tableResource, partitionMeta));
    case SPT_INDEXLIB:
        return SuezPartitionPtr(new SuezIndexPartition(tableResource, partitionMeta));
    case SPT_TABLET:
        return SuezPartitionPtr(new SuezTabletPartition(tableResource, partitionMeta));
    default:
        return SuezPartitionPtr();
    }
}

} // namespace suez
