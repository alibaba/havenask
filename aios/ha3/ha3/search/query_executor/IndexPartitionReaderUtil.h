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
#include <map>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace partition {
class IndexPartitionReader;
class PartitionReaderSnapshot;

typedef std::shared_ptr<IndexPartitionReader> IndexPartitionReaderPtr;
}  // namespace partition
}  // namespace indexlib

namespace isearch {
namespace search {

class IndexPartitionReaderUtil
{
public:
    IndexPartitionReaderUtil();
    ~IndexPartitionReaderUtil();
private:
    IndexPartitionReaderUtil(const IndexPartitionReaderUtil &);
    IndexPartitionReaderUtil& operator=(const IndexPartitionReaderUtil &);
public:
    static search::IndexPartitionReaderWrapperPtr createIndexPartitionReaderWrapper(
            indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
            const std::string& mainTableName, bool usePartial = false);

    static search::IndexPartitionReaderWrapperPtr createIndexPartitionReaderWrapper(
            const indexlib::partition::IndexPartitionReaderPtr &indexPartReader,
            bool usePartial = false);

private:
    static search::IndexPartitionReaderWrapperPtr createIndexPartitionReaderWrapper(
            const std::map<std::string, uint32_t> *indexName2IdMap,
            const std::map<std::string, uint32_t> *attrName2IdMap,
            const std::vector<indexlib::partition::IndexPartitionReaderPtr> *indexReaderVec,
            bool usePartial, bool ownMap);

    static void addIndexPartition(const indexlib::config::IndexPartitionSchemaPtr &schemaPtr,
                                  uint32_t id,
                                  std::map<std::string, uint32_t> &indexName2IdMap,
                                  std::map<std::string, uint32_t> &attrName2IdMap);


private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<IndexPartitionReaderUtil> IndexPartitionReaderUtilPtr;

} // namespace search
} // namespace isearch
