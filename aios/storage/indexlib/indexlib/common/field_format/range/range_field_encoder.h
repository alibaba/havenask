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
#ifndef __INDEXLIB_RANGE_FIELD_ENCODER_H
#define __INDEXLIB_RANGE_FIELD_ENCODER_H

#include <ctime>
#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexSchema);

namespace indexlib { namespace common {

class RangeFieldEncoder
{
public:
    RangeFieldEncoder(const config::IndexSchemaPtr& indexSchema);
    ~RangeFieldEncoder();

public:
    enum RangeFieldType { INT, UINT, UNKOWN };

    typedef std::pair<uint64_t, uint64_t> Range;
    typedef std::vector<Range> Ranges;

    void Init(const config::IndexSchemaPtr& indexSchema);
    bool IsRangeIndexField(fieldid_t fieldId);
    void Encode(fieldid_t fieldId, const std::string& fieldValue, std::vector<dictkey_t>& dictKeys);
    bool NormalizeToUInt64(fieldid_t fieldId, const std::string& fieldValue, uint64_t& value);
    static RangeFieldType GetRangeFieldType(FieldType fieldType);
    static void ConvertTerm(const index::Int64Term* term, RangeFieldType fieldType, uint64_t& leftTerm,
                            uint64_t& rightTerm);
    static void CalculateSearchRange(uint64_t leftTerm, uint64_t rightTerm, Ranges& bottomLevelRanges,
                                     Ranges& higherLevelRanges);
    static void AlignedTerm(bool alignLeft, bool alignRight, uint64_t& leftTerm, uint64_t& rightTerm);

private:
    std::vector<RangeFieldType> mFieldVec;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RangeFieldEncoder);
//////////////////////////////////////////////////////////////
inline void RangeFieldEncoder::ConvertTerm(const index::Int64Term* term, RangeFieldType fieldType, uint64_t& leftTerm,
                                           uint64_t& rightTerm)
{
    // TODO:when ha3 support uint64 change here
    if (fieldType == INT) {
        leftTerm = (1UL << 63) + term->GetLeftNumber();
        rightTerm = (1UL << 63) + term->GetRightNumber();
    } else {
        leftTerm = (uint64_t)term->GetLeftNumber();
        rightTerm = (uint64_t)term->GetRightNumber();
    }
}

inline void RangeFieldEncoder::AlignedTerm(bool alignLeft, bool alignRight, uint64_t& leftTerm, uint64_t& rightTerm)
{
    if (!alignLeft && !alignRight) {
        return;
    }

    uint64_t diffBits = 0;
    uint64_t tmpLeft = leftTerm;
    uint64_t tmpRight = rightTerm;
    while (tmpLeft != tmpRight && diffBits < 60) // not compare top level
    {
        tmpLeft >>= 4;
        tmpRight >>= 4;
        diffBits += 4;
    }

    if (alignLeft) {
        leftTerm &= (~((1UL << diffBits) - 1));
    }

    if (alignRight) {
        rightTerm |= ((1UL << diffBits) - 1);
    }
}

inline void RangeFieldEncoder::CalculateSearchRange(uint64_t leftTerm, uint64_t rightTerm, Ranges& bottomLevelRanges,
                                                    Ranges& higherLevelRanges)
{
    uint64_t mask = 15;
    Ranges* levelRanges = &bottomLevelRanges;
    for (uint64_t level = 0; level <= 15 && leftTerm <= rightTerm; level++) {
        bool hasLower = (leftTerm & mask) != 0;
        bool hasUpper = (rightTerm & mask) != mask;

        // if rightTerm / 16 equal 0, nextRightTerm would be negative. so transfer to int64_t
        int64_t nextLeftTerm = (int64_t)(leftTerm >> 4) + (hasLower ? 1 : 0);
        int64_t nextRightTerm = (int64_t)(rightTerm >> 4) - (hasUpper ? 1 : 0);

        if (level == 15 || nextLeftTerm > nextRightTerm) {
            uint64_t leftRange = leftTerm + (level << 60);
            uint64_t rightRange = rightTerm + (level << 60);
            levelRanges->emplace_back(leftRange, rightRange);
            break;
        }

        if (hasLower) {
            uint64_t leftRange = leftTerm + (level << 60);
            uint64_t rightRange = (leftTerm | mask) + (level << 60);
            levelRanges->emplace_back(leftRange, rightRange);
        }

        if (hasUpper) {
            uint64_t leftRange = (rightTerm & (~mask)) + (level << 60);
            uint64_t rightRange = rightTerm + (level << 60);
            levelRanges->emplace_back(leftRange, rightRange);
        }
        leftTerm = (uint64_t)nextLeftTerm;
        rightTerm = (uint64_t)nextRightTerm;
        if (level == 0) {
            levelRanges = &higherLevelRanges;
        }
    }
}

inline bool RangeFieldEncoder::NormalizeToUInt64(fieldid_t fieldId, const std::string& fieldValue, uint64_t& value)
{
    RangeFieldType type = mFieldVec[fieldId];
    if (type == INT) {
        int64_t num = 0;
        if (!autil::StringUtil::numberFromString(fieldValue, num)) {
            IE_LOG(DEBUG, "field value [%s] is not date number.", fieldValue.c_str());
            return false;
        }
        value = (1UL << 63) + num;
        return true;
    }
    assert(type == UINT);
    return autil::StringUtil::numberFromString(fieldValue, value);
}

inline void RangeFieldEncoder::Encode(fieldid_t fieldId, const std::string& fieldValue,
                                      std::vector<dictkey_t>& dictKeys)
{
    dictKeys.clear();
    if (!IsRangeIndexField(fieldId)) {
        return;
    }
    if (fieldValue.empty()) {
        IE_LOG(DEBUG, "field [%d] value is empty.", fieldId);
        return;
    }
    uint64_t value = 0;
    if (!NormalizeToUInt64(fieldId, fieldValue, value)) {
        return;
    }

    size_t levelNum = 16;
    dictKeys.reserve(levelNum);
    dictKeys.push_back(value);
    for (size_t levelIdx = 1; levelIdx < levelNum; levelIdx++) {
        value >>= 4;
        dictKeys.push_back(value + (levelIdx << 60));
    }
}

inline bool RangeFieldEncoder::IsRangeIndexField(fieldid_t fieldId)
{
    if (fieldId >= (fieldid_t)mFieldVec.size()) {
        return false;
    }
    return mFieldVec[fieldId] != UNKOWN;
}
}} // namespace indexlib::common

#endif //__INDEXLIB_RANGE_FIELD_ENCODER_H
