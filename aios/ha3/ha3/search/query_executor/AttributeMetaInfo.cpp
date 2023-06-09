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
#include "ha3/search/AttributeMetaInfo.h"

#include <iosfwd>

#include "autil/Log.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace indexlib::index;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, AttributeMetaInfo);

AttributeMetaInfo::AttributeMetaInfo(
        const string &attrName, 
        AttributeType attrType, 
        const IndexPartitionReaderPtr &indexPartReaderPtr)
    : _attrName(attrName)
    , _attrType(attrType)
    , _indexPartReaderPtr(indexPartReaderPtr)
{
}

AttributeMetaInfo::~AttributeMetaInfo() { 
}

} // namespace search
} // namespace isearch

