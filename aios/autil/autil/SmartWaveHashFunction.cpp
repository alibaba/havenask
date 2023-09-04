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
#include "autil/SmartWaveHashFunction.h"

#include "autil/HashAlgorithm.h"
#include "autil/HashFuncFactory.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"

using namespace std;
namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, SmartWaveHashFunction);

static const string BIGDATA_VALUES_KEY = "bigdata_values";
static const string BIGDATA_VALUES_SPLIT_KEY = ",";
static const string PART_NUM_KEY = "partition_count";

bool SmartWaveHashFunction::init() {
    auto iter = _params.find(BIGDATA_VALUES_KEY);

    vector<string> bigDataVec;
    if (iter != _params.end()) {
        string bigDataValues = "";
        if (!StringUtil::fromString(iter->second, bigDataValues)) {
            AUTIL_LOG(WARN, "parse param bigdata_values [%s] failed", iter->second.c_str());
            return false;
        }

        bigDataVec = StringUtil::split(bigDataValues, BIGDATA_VALUES_SPLIT_KEY);
        if (bigDataVec.size() <= 1) {
            AUTIL_LOG(WARN,
                      "the number of bigdata values [%s] must be greater than 1, maybe the split key is wrong, should "
                      "be [%s].",
                      iter->second.c_str(),
                      BIGDATA_VALUES_SPLIT_KEY.c_str());
            return false;
        }

    } else {
        AUTIL_LOG(WARN, "the param bigdata_values must be configed!");
        return false;
    }

    auto partitionIter = _params.find(PART_NUM_KEY);
    if (partitionIter != _params.end()) {
        if (!StringUtil::fromString(partitionIter->second, _partNum)) {
            AUTIL_LOG(WARN, "parse partition_count [%s] failed", partitionIter->second.c_str());
            return false;
        }
        if (_partNum <= 1 || _partNum > _hashSize) {
            AUTIL_LOG(
                WARN, "partition_count [%d] must be greater than 1 and smaller than [%d]", _partNum, _hashSize + 1);
            return false;
        }
    } else {
        AUTIL_LOG(WARN, "the param partition_count must be configed!");
        return false;
    }

    _defaultHashFuncPtr.reset(new DefaultHashFunction("HASH", _hashSize));
    if (!_defaultHashFuncPtr) {
        AUTIL_LOG(WARN, "DefaultHashFunction in SmartWaveHash init failed");
        return false;
    }

    uint32_t c = _hashSize / _partNum;
    uint32_t m = _hashSize % _partNum;

    uint32_t from = 0;
    for (size_t i = 0; i < bigDataVec.size(); i++) {
        if (from >= _hashSize) {
            from = 0;
        }
        uint32_t index = i % _partNum;
        uint32_t to = from + c + (index >= m ? 0 : 1) - 1;
        _bigDataMap.insert(std::make_pair(bigDataVec[i], from));
        from = to + 1;
    }

    return true;
}

uint32_t SmartWaveHashFunction::getHashId(const std::string &str) const {
    auto iter = _bigDataMap.find(str);
    if (iter != _bigDataMap.end()) {
        return iter->second;
    }
    return _defaultHashFuncPtr->getHashId(str);
}

} // namespace autil
