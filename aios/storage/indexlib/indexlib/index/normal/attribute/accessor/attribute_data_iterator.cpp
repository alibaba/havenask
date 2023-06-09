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
#include "indexlib/index/normal/attribute/accessor/attribute_data_iterator.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeDataIterator);

AttributeDataIterator::AttributeDataIterator(const AttributeConfigPtr& attrConfig)
    : mAttrConfig(attrConfig)
    , mDocCount(0)
    , mCurDocId(0)
{
    assert(mAttrConfig);
}

AttributeDataIterator::~AttributeDataIterator() {}
}} // namespace indexlib::index
