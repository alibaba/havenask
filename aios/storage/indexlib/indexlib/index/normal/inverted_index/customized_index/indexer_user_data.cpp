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
#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"

using namespace std;

namespace indexlib {

IndexerUserData::IndexerUserData() {}

IndexerUserData::~IndexerUserData() {}

bool IndexerUserData::init(const vector<IndexerSegmentData>& segDatas) { return true; }

size_t IndexerUserData::estimateInitMemUse(const vector<IndexerSegmentData>& segDatas) const { return 0; }

void IndexerUserData::setData(const string& key, const string& value) { mDataMap[key] = value; }

bool IndexerUserData::getData(const string& key, string& value) const
{
    auto iter = mDataMap.find(key);
    if (iter != mDataMap.end()) {
        value = iter->second;
        return true;
    }
    return false;
}

} // namespace indexlib
