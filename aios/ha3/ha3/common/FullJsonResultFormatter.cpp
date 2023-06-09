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
#include "ha3/common/FullJsonResultFormatter.h"
#include "ha3/common/CommonDef.h"
#include "ha3/util/XMLFormatUtil.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "autil/ConstString.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

using namespace std;
using namespace autil;
using namespace suez::turing;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, FullJsonResultFormatter);
using namespace isearch::util;

const unsigned char UTF8_HEAD_BYTES[256] = {
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //0x00
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //0x10
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //0x20
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //0x30
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //0x40
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //0x50
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //0x60
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, //1x70
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //1x80
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //1x90
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //1xA0
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, //1xB9
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, //1xC0
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, //0xD0
    3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, //0xE0
    4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 0, 0  //0xF0
};

const uint32_t UTF8_HEAD_MASK[7] = {
    0x00,                           /* unused */
    0x7F,                           /* 1 byte */
    0x1F,                           /* 2 bytes */
    0x0F,                           /* 3 bytes */
    0x07,                           /* 4 bytes */
    0x03,                           /* 5 bytes */
    0x01,                           /* 6 bytes */
};

#define FILL_VALUE(type_name)                                           \
    if (!attrItem->isMultiValue()) {                                    \
        const AttributeItemTyped<type_name> *attrItemTyped = static_cast<const AttributeItemTyped<type_name> *>(attrItem); \
        assert(attrItemTyped);                                          \
        const type_name &v = attrItemTyped->getItem();                  \
        rapidjson::Value name(attrName.c_str(), attrName.size(), allocator); \
        value.AddMember(name, v, allocator);                            \
    } else {                                                            \
        rapidjson::Value arrayAvalue;                                   \
        arrayAvalue.SetArray();                                         \
        const AttributeItemTyped<vector<type_name> > *attrItemTyped = static_cast<const AttributeItemTyped<vector<type_name> > *>(attrItem); \
        assert(attrItemTyped);                                          \
        const vector<type_name> &multiValue = attrItemTyped->getItem(); \
        for (size_t i = 0; i < multiValue.size(); ++i) {                \
            arrayAvalue.PushBack(multiValue[i], allocator);             \
        }                                                               \
        rapidjson::Value name(attrName.c_str(), attrName.size(), allocator);\
        value.AddMember(name, arrayAvalue, allocator);  \
    }

#define FILL_STRING_VALUE(type_name)                                    \
    if (!attrItem->isMultiValue()) {                                    \
        const AttributeItemTyped<type_name> *attrItemTyped = static_cast<const AttributeItemTyped<type_name> *>(attrItem); \
        assert(attrItemTyped);                                          \
        const type_name &str = attrItemTyped->getItem();                  \
        rapidjson::Value name(attrName.c_str(), attrName.size(), allocator); \
        rapidjson::Value newValue(str.c_str(), str.size(), allocator);  \
        value.AddMember(name, newValue, allocator);                     \
    } else {                                                            \
        rapidjson::Value arrayAvalue;                                   \
        arrayAvalue.SetArray();                                         \
        const AttributeItemTyped<vector<type_name> > *attrItemTyped = static_cast<const AttributeItemTyped<vector<type_name> > *>(attrItem); \
        assert(attrItemTyped);                                          \
        const vector<type_name> &multiValue = attrItemTyped->getItem(); \
        for (size_t i = 0; i < multiValue.size(); ++i) {                \
            const type_name &str = multiValue[i];                           \
            rapidjson::Value newValue(str.c_str(), str.size(), allocator);  \
            arrayAvalue.PushBack(newValue, allocator);                  \
        }                                                               \
        rapidjson::Value name(attrName.c_str(), attrName.size(), allocator);\
        value.AddMember(name, arrayAvalue, allocator);  \
    }

#define FILL_ATTRIBUTE(vt)                                              \
    case vt:                                                            \
    {                                                                   \
        typedef VariableTypeTraits<vt, false>::AttrItemType T;          \
        FILL_VALUE(T);                                                  \
        break;                                                          \
    }

FullJsonResultFormatter::FullJsonResultFormatter() {
}

FullJsonResultFormatter::~FullJsonResultFormatter() {
}

void FullJsonResultFormatter::format(const ResultPtr &resultPtr, stringstream &ss) {
    rapidjson::Document document;
    document.SetObject();
    rapidjson::Document::AllocatorType &allocator = document.GetAllocator();

    rapidjson::Value resultValue;
    formatResult(resultPtr, resultValue, allocator);
    document.AddMember("result", resultValue, allocator);
    formatMeta(resultPtr, document);

    formatErrorResult(resultPtr, document);
    formatRequestTracer(resultPtr, document);

    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document.Accept(writer);
    ss << buffer.GetString();
}

void FullJsonResultFormatter::formatResult(const ResultPtr &resultPtr,
        rapidjson::Value &resultValue,
        rapidjson::Document::AllocatorType &allocator)
{
    resultValue.SetObject();
    formatGlobalInfo(resultPtr, resultValue, allocator);
    formatHits(resultPtr, resultValue, allocator);
    formatAggregateResults(resultPtr, resultValue, allocator);
}

void FullJsonResultFormatter::formatGlobalInfo(const ResultPtr &resultPtr,
        rapidjson::Value &resultValue,
        rapidjson::Document::AllocatorType &allocator)
{
    double totalTime = (double)(resultPtr->getTotalTime()/(double)1000000.0);
    resultValue.AddMember("searchtime", totalTime, allocator);
    uint32_t hitCount = 0;
    auto hits = resultPtr->getHits();
    if (hits) {
        hitCount = hits->size();
    }
    const Result::ClusterPartitionRanges &mergedRanges =
        resultPtr->getCoveredRanges();
    float coveredPercent = getCoveredPercent(mergedRanges);
    resultValue.AddMember("numHits", hitCount, allocator);
    resultValue.AddMember("totalHits", resultPtr->getTotalHits(), allocator);
    resultValue.AddMember("coveredPercent", coveredPercent, allocator);
}

void FullJsonResultFormatter::formatHits(const ResultPtr &resultPtr,
        rapidjson::Value &resultValue,
        rapidjson::Document::AllocatorType &allocator)
{
    Hits *hits = resultPtr->getHits();
    if (hits == NULL) {
        AUTIL_LOG(WARN, "There are no hits in the result");
        return;
    }
    rapidjson::Value itemValue;
    itemValue.SetArray();
    uint32_t hitSize = hits->size();
    for (uint32_t i = 0; i < hitSize; i++) {
        HitPtr hitPtr = hits->getHit(i);
        rapidjson::Value hitValue;
        formatHit(hitPtr, hitValue, allocator);
        itemValue.PushBack(hitValue, allocator);
    }
    resultValue.AddMember("items", itemValue, allocator);

    auto &metaHitMap = hits->getMetaHitMap();
    if (!metaHitMap.empty()) {
        rapidjson::Value metaHitValues;
        formatMetaHits(metaHitMap, metaHitValues, allocator);
        resultValue.AddMember("metaHits", metaHitValues, allocator);
    }
}

void FullJsonResultFormatter::formatHit(const HitPtr &hitPtr,
        rapidjson::Value &hitValue,
        rapidjson::Document::AllocatorType &allocator)
{
    hitValue.SetObject();
    rapidjson::Value summary;
    formatSummaryFields(hitPtr, summary, allocator);
    hitValue.AddMember("fields", summary, allocator);

    rapidjson::Value propertyValue;
    formatPropety(hitPtr, propertyValue, allocator);
    hitValue.AddMember("properties", propertyValue, allocator);

    rapidjson::Value attrsValue;
    formatAttributes(hitPtr, attrsValue, allocator);
    hitValue.AddMember("attributes", attrsValue, allocator);

    rapidjson::Value variablesValue;
    formatVariables(hitPtr, variablesValue, allocator);
    hitValue.AddMember("variableValues", variablesValue, allocator);

    rapidjson::Value sortExprValues;
    formatSortExprValues(hitPtr, sortExprValues, allocator);
    hitValue.AddMember("sortExprValues", sortExprValues, allocator);

    auto tracer = hitPtr->getTracer();
    if (tracer) {
        hitValue.AddMember("tracerInfo", tracer->getTraceInfo(), allocator);
    }
}

void FullJsonResultFormatter::formatSummaryFields(const HitPtr &hitPtr,
        rapidjson::Value &summary, rapidjson::Document::AllocatorType &allocator)
{
    summary.SetObject();
    SummaryHit *summaryHit = hitPtr->getSummaryHit();
    if (summaryHit) {
        const config::HitSummarySchema *hitSummarySchema =
            summaryHit->getHitSummarySchema();
        size_t summaryFieldCount = summaryHit->getFieldCount();
        for (size_t i = 0; i < summaryFieldCount; ++i) {
            const string &fieldName = hitSummarySchema->getSummaryFieldInfo(i)->fieldName;
            const StringView *str = summaryHit->getFieldValue(i);
            if (!str) {
                continue;
            }
            string srcStr(str->data(), str->size());
            string dstStr;
            formatFieldValue(srcStr, dstStr);
            rapidjson::Value fieldValue(dstStr.c_str(), dstStr.size(), allocator);
            rapidjson::Value name(fieldName.c_str(), fieldName.size(), allocator);
            summary.AddMember(name, fieldValue, allocator);
        }
    }
}

void FullJsonResultFormatter::formatPropety(const HitPtr &hitPtr,
        rapidjson::Value &propertyValue,
        rapidjson::Document::AllocatorType &allocator)
{
    propertyValue.SetObject();
    const PropertyMap& propertyMap = hitPtr->getPropertyMap();
    for (PropertyMap::const_iterator it = propertyMap.begin();
         it != propertyMap.end(); ++it)
    {
        string dstStr;
        formatFieldValue(it->second, dstStr);
        rapidjson::Value fieldValue(dstStr.c_str(), dstStr.size(), allocator);
        rapidjson::Value name(it->first.c_str(), it->first.size(), allocator);
        propertyValue.AddMember(name, fieldValue, allocator);
    }
}

void FullJsonResultFormatter::formatAttributes(const HitPtr &hitPtr,
        rapidjson::Value &attrsValue, rapidjson::Document::AllocatorType &allocator)
{
    attrsValue.SetObject();
    auto& attrMap = hitPtr->getAttributeMap();
    for (auto it = attrMap.begin(); it != attrMap.end(); ++it) {
        formatAttribute(it->first, it->second.get(), attrsValue, allocator);
    }
}

void FullJsonResultFormatter::formatVariables(const HitPtr &hitPtr,
        rapidjson::Value &variablesValue, rapidjson::Document::AllocatorType &allocator)
{
    variablesValue.SetObject();
    const AttributeMap& variableValueMap = hitPtr->getVariableValueMap();
    for (AttributeMap::const_iterator it = variableValueMap.begin();
         it != variableValueMap.end(); ++it)
    {
        formatAttribute(it->first, it->second.get(), variablesValue, allocator);
    }
}

void FullJsonResultFormatter::formatMetaHits(const MetaHitMap& metaHitMap,
        rapidjson::Value &metaHitValues,
        rapidjson::Document::AllocatorType &allocator)
{
    metaHitValues.SetArray();
    for (auto it = metaHitMap.begin(); it != metaHitMap.end(); ++it) {
        auto& metaHit = it->second;
        if (metaHit.empty()) {
            continue;
        }
        rapidjson::Value metaHitValue;
        metaHitValue.SetObject();
        metaHitValue.AddMember("key", it->first, allocator);
        rapidjson::Value values;
        values.SetObject();
        for (auto it2 = metaHit.begin(); it2 != metaHit.end(); ++it2) {
            rapidjson::Value name(it2->first.c_str(), it2->first.size(), allocator);
            rapidjson::Value metaValue(it2->second.c_str(), it2->second.size(), allocator);
            values.AddMember(name, metaValue, allocator);
        }
        metaHitValue.AddMember("items", values, allocator);
        metaHitValues.PushBack(metaHitValue, allocator);
    }
}

void FullJsonResultFormatter::formatSortExprValues(const HitPtr &hitPtr,
        rapidjson::Value &sortExpValues, rapidjson::Document::AllocatorType &allocator)
{
    sortExpValues.SetArray();
    for (uint32_t i = 0; i < hitPtr->getSortExprCount(); ++i) {
        const auto& sortExprStr = hitPtr->getSortExprValue(i);
        rapidjson::Value sortValue(sortExprStr.c_str(), sortExprStr.size(), allocator);
        sortExpValues.PushBack(sortValue, allocator);
    }
}

void FullJsonResultFormatter::formatAttribute(const string& attrName,
        const AttributeItem* attrItem, rapidjson::Value &value,
        rapidjson::Document::AllocatorType &allocator)
{
    if (!attrItem) {
        return;
    }
    auto vt = attrItem->getType();
    switch(vt) {
        FILL_ATTRIBUTE(vt_bool);
        FILL_ATTRIBUTE(vt_int8);
        FILL_ATTRIBUTE(vt_int16);
        FILL_ATTRIBUTE(vt_int32);
        FILL_ATTRIBUTE(vt_int64);
        FILL_ATTRIBUTE(vt_uint8);
        FILL_ATTRIBUTE(vt_uint16);
        FILL_ATTRIBUTE(vt_uint32);
        FILL_ATTRIBUTE(vt_uint64);
        FILL_ATTRIBUTE(vt_float);
        FILL_ATTRIBUTE(vt_double);
    case vt_string:
    {
        FILL_STRING_VALUE(string);
        break;
    }
    case vt_hash_128:
    {
        assert(!attrItem->isMultiValue());
        const AttributeItemTyped<primarykey_t> *attrItemTyped =
            static_cast<const AttributeItemTyped<primarykey_t> *>(attrItem);
        assert(attrItemTyped);
        const primarykey_t &v = attrItemTyped->getItem();
        rapidjson::Value name(attrName.c_str(), attrName.size(), allocator);
        string attrStr = StringUtil::toString(v);
        rapidjson::Value attrValue(attrStr.c_str(), attrStr.size(), allocator);
        value.AddMember(name, attrValue, allocator);
        break;
    }
    default:
        assert(false);
        break;
    }
}

void FullJsonResultFormatter::formatFieldValue(const string& srcStr,
        string& dstStr)
{
    dstStr.reserve(srcStr.size() << 1);
    size_t pos = 0;
    while(pos < srcStr.length()) {
        if (XMLFormatUtil::isInvalidXMLChar(srcStr[pos])) {
            dstStr.append(1, '\t');
        } else {
            size_t lastPos = pos;
            // only valid chars are appended to result.
            if (consumeValidChar(srcStr, pos)) {
                size_t consumeLength = pos - lastPos;
                dstStr.append(srcStr, lastPos, consumeLength);
            }
        }
    }
}

bool FullJsonResultFormatter::consumeValidChar(const string &text, size_t &pos) {
    bool ret = false;
    uint32_t unicode = 0;
    if (getUnicodeChar(text, pos, unicode)) {
        if (isValidUnicodeValue(unicode)) {
            ret = true;
        }
    }

    return ret;
}

bool FullJsonResultFormatter::getUnicodeChar(const string &utf8Str,
        size_t &pos, uint32_t &unicode)
{
    uint8_t ch = utf8Str[pos++];
    uint8_t length = UTF8_HEAD_BYTES[ch];
    if (0 == length) {
        return false;
    }

    unicode = ch & UTF8_HEAD_MASK[length];
    uint8_t utf8CharCnt = 0;
    while((++utf8CharCnt) < length && pos != utf8Str.size()) {
        ch = utf8Str[pos];
        if ((ch & 0xC0) != 0x80){
            return false;
        }
        pos++;
        unicode = (unicode << 6) | (ch & 0x3F);
    }
    return utf8CharCnt == length;
}

bool FullJsonResultFormatter::isValidUnicodeValue(uint32_t unicode) {
    // These are discourge characters
    if (((unicode >= 0x7F) && (unicode <= 0x84)) ||
        ((unicode >= 0x86) && (unicode <= 0x9F)) ||
        ((unicode >= 0xFDD0) && (unicode <= 0xFDEF)) ||
        ((unicode >= 0x1FFFE) && (unicode <= 0x1FFFF)) ||
        ((unicode >= 0x2FFFE) && (unicode <= 0x2FFFF)) ||
        ((unicode >= 0x3FFFE) && (unicode <= 0x3FFFF)) ||
        ((unicode >= 0x4FFFE) && (unicode <= 0x4FFFF)) ||
        ((unicode >= 0x5FFFE) && (unicode <= 0x5FFFF)) ||
        ((unicode >= 0x6FFFE) && (unicode <= 0x6FFFF)) ||
        ((unicode >= 0x7FFFE) && (unicode <= 0x7FFFF)) ||
        ((unicode >= 0x8FFFE) && (unicode <= 0x8FFFF)) ||
        ((unicode >= 0x9FFFE) && (unicode <= 0x9FFFF)) ||
        ((unicode >= 0xAFFFE) && (unicode <= 0xAFFFF)) ||
        ((unicode >= 0xBFFFE) && (unicode <= 0xBFFFF)) ||
        ((unicode >= 0xCFFFE) && (unicode <= 0xCFFFF)) ||
        ((unicode >= 0xDFFFE) && (unicode <= 0xDFFFF)) ||
        ((unicode >= 0xEFFFE) && (unicode <= 0xEFFFF)) ||
        ((unicode >= 0xFFFFE) && (unicode <= 0xFFFFF)) ||
        ((unicode >= 0x10FFFE) && (unicode <= 0x10FFFF))) {
        return false;
    }
    // This is xml standard
    if (((unicode >= 0x20) && (unicode <= 0xD7FF)) ||
        ((unicode >= 0xE000) && (unicode <= 0xFFFD)) ||
        (unicode == 0x9) ||(unicode == 0xA) || (unicode == 0xD) ||
        ((unicode >= 0x10000) && (unicode <= 0x10FFFF))) {
        return true;
    }

    return false;
}

void FullJsonResultFormatter::formatAggregateResults(const ResultPtr &resultPtr,
        rapidjson::Value &resultValue,
        rapidjson::Document::AllocatorType &allocator)
{
    rapidjson::Value aggResultValues;
    aggResultValues.SetArray();
    const AggregateResults& aggRes = resultPtr->getAggregateResults();
    for (AggregateResults::const_iterator it = aggRes.begin();
         it != aggRes.end(); it++)
    {
        formatAggregateResult(*it, aggResultValues, allocator);
    }
    resultValue.AddMember("facet", aggResultValues, allocator);
}

void FullJsonResultFormatter::formatAggregateResult(
        AggregateResultPtr aggResultPtr,
        rapidjson::Value &aggResultValues,
        rapidjson::Document::AllocatorType &allocator)
{
    rapidjson::Value aggResultValue;
    aggResultValue.SetObject();

    const auto &aggResultVector = aggResultPtr->getAggResultVector();
    const auto &matchDocAllocatorPtr = aggResultPtr->getMatchDocAllocator();
    assert(matchDocAllocatorPtr);
    auto ref = matchDocAllocatorPtr->findReferenceWithoutType(common::GROUP_KEY_REF);
    assert(ref);
    auto referenceVec = matchDocAllocatorPtr->getAllNeedSerializeReferences(SL_QRS);
    uint32_t aggFunCount = aggResultPtr->getAggFunCount();
    assert(aggFunCount + 1 == referenceVec.size());

    auto& exprStr = aggResultPtr->getGroupExprStr();
    rapidjson::Value groupExprValue(exprStr.c_str(), exprStr.size(), allocator);
    aggResultValue.AddMember("key", groupExprValue, allocator);

    rapidjson::Value aggGroupValues;
    aggGroupValues.SetArray();

    for (matchdoc::MatchDoc aggFunResults : aggResultVector) {
        rapidjson::Value aggGroupValue;
        aggGroupValue.SetObject();
        const string &aggExprValueStr = ref->toString(aggFunResults);
        rapidjson::Value aggExprValue(aggExprValueStr.c_str(), aggExprValueStr.size(), allocator);
        aggGroupValue.AddMember("value", aggExprValue, allocator);

        for (uint32_t i=1; i<=aggFunCount; i++) {
            const string& funcNameStr = aggResultPtr->getAggFunName(i-1);
            string funcValueStr = referenceVec[i]->toString(aggFunResults);
            rapidjson::Value funcName(funcNameStr.c_str(), funcNameStr.size(), allocator);
            rapidjson::Value funcValue(funcValueStr.c_str(), funcValueStr.size(), allocator);
            aggGroupValue.AddMember(funcName, funcValue, allocator);
        }
        aggGroupValues.PushBack(aggGroupValue, allocator);
    }
    aggResultValue.AddMember("items", aggGroupValues, allocator);

    aggResultValues.PushBack(aggResultValue, allocator);
}

void FullJsonResultFormatter::formatErrorResult(const ResultPtr & resultPtr,
        rapidjson::Document &document)
{
    rapidjson::Value errorResultValues;
    errorResultValues.SetArray();
    rapidjson::Document::AllocatorType &allocator = document.GetAllocator();
    auto& multiErrorResult = resultPtr->getMultiErrorResult();
    auto& errorResults = multiErrorResult->getErrorResults();
    for (auto& errorRes : errorResults) {
        rapidjson::Value errorResultVaule;
        errorResultVaule.SetObject();
        errorResultVaule.AddMember("code", errorRes.getErrorCode(), allocator);
        auto errorDesStr = errorRes.getErrorDescription();
        rapidjson::Value errorDesValue(errorDesStr.c_str(), errorDesStr.size(), allocator);
        errorResultVaule.AddMember("message", errorDesValue, allocator);
        errorResultValues.PushBack(errorResultVaule, allocator);
    }
    document.AddMember("errors", errorResultValues, allocator);
}

void FullJsonResultFormatter::formatRequestTracer(const ResultPtr &resultPtr,
        rapidjson::Document &document)
{
    Tracer *tracer = resultPtr->getTracer();
    if (tracer) {
        rapidjson::Document::AllocatorType &allocator = document.GetAllocator();
        document.AddMember("requestTrace", tracer->getTraceInfo(), allocator);
    }
}

void FullJsonResultFormatter::formatMeta(const ResultPtr &resultPtr,
        rapidjson::Document &document)
{
    auto& metaMap = resultPtr->getMetaMap();
    if (metaMap.empty()) {
        return;
    }
    AUTIL_LOG(WARN, "meta map size: %d", (int)metaMap.size());
    rapidjson::Document::AllocatorType &allocator = document.GetAllocator();
    rapidjson::Value metaValues;
    metaValues.SetArray();
    for (auto it = metaMap.begin(); it != metaMap.end(); ++it) {
        rapidjson::Value metaValue;
        metaValue.SetObject();
        metaValue.AddMember("key", it->first, allocator);
        rapidjson::Value valueMap;
        valueMap.SetObject();
        auto &metas = it->second;
        for (auto it2 = metas.begin(); it2 != metas.end(); ++it2) {
            rapidjson::Value key(it2->first.c_str(), it2->first.size(), allocator);
            rapidjson::Value value(it2->second.c_str(), it2->second.size(), allocator);
            valueMap.AddMember(key, value, allocator);
        }
        metaValue.AddMember("meta", valueMap, allocator);
        metaValues.PushBack(metaValue, allocator);
    }
    document.AddMember("metas", metaValues, allocator);

}

} //end namespace common
} //end namespace isearch
