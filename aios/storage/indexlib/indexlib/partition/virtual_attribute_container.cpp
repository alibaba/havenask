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
#include "indexlib/partition/virtual_attribute_container.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, VirtualAttributeContainer);

VirtualAttributeContainer::VirtualAttributeContainer() {}

VirtualAttributeContainer::~VirtualAttributeContainer() {}

void VirtualAttributeContainer::UpdateUsingAttributes(const vector<pair<string, bool>>& attributes)
{
    for (size_t i = 0; i < mUsingAttributes.size(); i++) {
        bool exist = false;
        for (size_t j = 0; j < attributes.size(); j++) {
            if (attributes[j] == mUsingAttributes[i]) {
                exist = true;
                break;
            }
        }
        if (!exist) {
            mUnusingAttributes.push_back(mUsingAttributes[i]);
        }
    }
    mUsingAttributes = attributes;
}

void VirtualAttributeContainer::UpdateUnusingAttributes(const vector<pair<string, bool>>& attributes)
{
    mUnusingAttributes = attributes;
}

const vector<pair<string, bool>>& VirtualAttributeContainer::GetUsingAttributes() { return mUsingAttributes; }

const vector<pair<string, bool>>& VirtualAttributeContainer::GetUnusingAttributes() { return mUnusingAttributes; }
}} // namespace indexlib::partition
