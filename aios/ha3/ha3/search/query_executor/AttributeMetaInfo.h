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

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/misc/common.h"
#include "indexlib/partition/index_partition_reader.h"

namespace isearch {
namespace search {

enum AttributeType {
    AT_MAIN_ATTRIBUTE,
    AT_SUB_ATTRIBUTE,
    AT_UNKNOWN,
};

class AttributeMetaInfo
{
public:
    typedef indexlib::partition::IndexPartitionReaderPtr IndexPartitionReaderPtr;
    typedef indexlib::index::AttributeReaderPtr AttributeReaderPtr;
public:
    AttributeMetaInfo(
            const std::string &attrName = "", 
            AttributeType attrType = AT_MAIN_ATTRIBUTE,
            const IndexPartitionReaderPtr &indexPartReaderPtr = IndexPartitionReaderPtr());
    ~AttributeMetaInfo();
public:
    const std::string& getAttributeName() const {
        return _attrName;
    }
    void setAttributeName(const std::string &attributeName) {
        _attrName = attributeName;
    }
    AttributeType getAttributeType() const {
        return _attrType;
    }
    void setAttributeType(AttributeType attrType) {
        _attrType = attrType;
    }
    const IndexPartitionReaderPtr& getIndexPartReader() const {
        return _indexPartReaderPtr;
    }
    void setIndexPartReader(const IndexPartitionReaderPtr& idxPartReaderPtr) {
        _indexPartReaderPtr = idxPartReaderPtr;
    }
    
private:
    std::string _attrName;
    AttributeType _attrType;
    IndexPartitionReaderPtr _indexPartReaderPtr;    
private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<AttributeMetaInfo> AttributeMetaInfoPtr;

} // namespace search
} // namespace isearch

