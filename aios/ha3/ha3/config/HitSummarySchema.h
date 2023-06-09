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

#include <stddef.h>
#include <stdint.h>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "autil/DataBuffer.h"
#include "indexlib/indexlib.h"

#include "autil/Log.h" // IWYU pragma: keep

namespace suez {
namespace turing {
class TableInfo;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace config {

class SummaryFieldInfo {
public:
    SummaryFieldInfo()
        : isMultiValue(false)
        , fieldType(ft_unknown)
        , summaryFieldId(INVALID_SUMMARYFIELDID)
    {
    }
    ~SummaryFieldInfo() {}

    void serialize(autil::DataBuffer &dataBuffer) const {
        dataBuffer.write(isMultiValue);
        dataBuffer.write(fieldType);
        dataBuffer.write(summaryFieldId);
        dataBuffer.write(fieldName);
    }
    
    void deserialize(autil::DataBuffer &dataBuffer) {
        dataBuffer.read(isMultiValue);
        dataBuffer.read(fieldType);
        dataBuffer.read(summaryFieldId);
        dataBuffer.read(fieldName);
    }

public:
    bool isMultiValue;
    FieldType fieldType;
    summaryfieldid_t summaryFieldId;
    std::string fieldName;
};

class HitSummarySchema
{
public:
    HitSummarySchema(const suez::turing::TableInfo *tableInfo = NULL);
    ~HitSummarySchema();
    HitSummarySchema(const HitSummarySchema &);
public:
    summaryfieldid_t declareSummaryField(const std::string &fieldName,
            FieldType fieldType = ft_string, bool isMultiValue = false);

    const SummaryFieldInfo *getSummaryFieldInfo(const std::string &fieldName) const;
    const SummaryFieldInfo *getSummaryFieldInfo(size_t pos) const;

    summaryfieldid_t getSummaryFieldId(const std::string &fieldName) const;

    size_t getFieldCount() const;
    void clearIncSchema();

private:
    HitSummarySchema& operator=(const HitSummarySchema &);
public:
    int64_t getSignature();
    int64_t getSignatureNoCalc() const;
    HitSummarySchema *clone() const;    
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
private:
    void init(const suez::turing::TableInfo *tableInfo);
    summaryfieldid_t declareIncSummaryField(const std::string &fieldName,
            FieldType fieldType, bool isMultiValue, size_t offset);
    void calcSignature();
    void updateOffset(size_t offset);
private:
    typedef std::map<std::string, size_t> SummaryFieldMap;
private:
    bool _needCalcSignature;
    int64_t _signature;
    SummaryFieldMap _summaryFieldInfoMap;
    std::vector<SummaryFieldInfo> _summaryFieldInfos;
    HitSummarySchema *_incSchema;
private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<HitSummarySchema> HitSummarySchemaPtr;
//////////////////////////////////////////////////////////////////

class HitSummarySchemaPool {

public:
    HitSummarySchemaPool(const HitSummarySchemaPtr &base, uint32_t count = 64);
    ~HitSummarySchemaPool();
private:
    HitSummarySchemaPool(const HitSummarySchemaPool &) = delete;
    HitSummarySchemaPool &operator=(const HitSummarySchemaPool &) = delete;
public:
    HitSummarySchemaPtr get();
    void put(const HitSummarySchemaPtr &schema);
private:
    HitSummarySchemaPtr _base;
    std::vector<HitSummarySchemaPtr> _schemaVec;
    mutable std::mutex _lock;
};
typedef std::shared_ptr<HitSummarySchemaPool> HitSummarySchemaPoolPtr;
//////////////////////////////////////////////////////////////////


inline const SummaryFieldInfo *HitSummarySchema::getSummaryFieldInfo(const std::string &fieldName) const {
    SummaryFieldMap::const_iterator it = _summaryFieldInfoMap.find(fieldName);
    if (it != _summaryFieldInfoMap.end()) {
        return &_summaryFieldInfos[it->second];
    }

    if (_incSchema) {
        return _incSchema->getSummaryFieldInfo(fieldName);
    }

    return NULL;    
}

inline const SummaryFieldInfo *HitSummarySchema::getSummaryFieldInfo(size_t pos) const {
    if (pos < _summaryFieldInfos.size()) {
        return &_summaryFieldInfos[pos];
    } else if (_incSchema) {
        return _incSchema->getSummaryFieldInfo(pos - _summaryFieldInfos.size());
    }
    return NULL;
}

inline summaryfieldid_t HitSummarySchema::getSummaryFieldId(const std::string &fieldName) const {
    SummaryFieldMap::const_iterator it = _summaryFieldInfoMap.find(fieldName);
    if (it != _summaryFieldInfoMap.end()) {
        return _summaryFieldInfos[it->second].summaryFieldId;
    }

    if (_incSchema) {
        return _incSchema->getSummaryFieldId(fieldName);
    }
    return INVALID_SUMMARYFIELDID;
}

} // namespace config
} // namespace isearch

