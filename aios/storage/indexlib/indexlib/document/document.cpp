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
#include "indexlib/document/document.h"

#include "autil/EnvUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/util/Exception.h"

using std::string;
using namespace std;

namespace indexlib { namespace document {

namespace {
static const bool ENABLE_SIMPLE_TAG_INFO_DECODE = []() {
    return autil::EnvUtil::getEnv("indexlib_enable_simple_tag_info_decode", 1);
}();
}

Document::Document()
    : mOpType(UNKNOWN_OP)
    , mOriginalOpType(UNKNOWN_OP)
    , mSerializedVersion(INVALID_DOC_VERSION)
    , mTTL(0)
    , mTimestamp(INVALID_TIMESTAMP)
    , mDocInfo(0, INVALID_TIMESTAMP, 0)
    , mTrace(false)
{
}

Document::Document(const Document& other)
    : mOpType(other.mOpType)
    , mOriginalOpType(other.mOriginalOpType)
    , mSerializedVersion(other.mSerializedVersion)
    , mTTL(other.mTTL)
    , mTimestamp(other.mTimestamp)
    , mDocInfo(other.mDocInfo)
    , mTagInfo(other.mTagInfo)
    , mLocator(other.mLocator)
    , mLocatorV2(other.mLocatorV2)
    , mTrace(other.mTrace)
{
}

Document::~Document() {}

bool Document::operator==(const Document& doc) const
{
    if (this == &doc) {
        return true;
    }

    if (GetSerializedVersion() != doc.GetSerializedVersion()) {
        return false;
    }

    if (mTimestamp != doc.mTimestamp) {
        return false;
    }
    if (mTagInfo != doc.mTagInfo) {
        return false;
    }
    if (mLocator != doc.mLocator) {
        return false;
    }
    if (mLocatorV2.IsFasterThan(doc.mLocatorV2, false) !=
            indexlibv2::framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER ||
        doc.mLocatorV2.IsFasterThan(mLocatorV2, false) !=
            indexlibv2::framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER) {
        return false;
    }
    if (mOpType != doc.mOpType) {
        return false;
    }
    if (mTTL != doc.mTTL) {
        return false;
    }
    if (mOriginalOpType != doc.mOriginalOpType) {
        return false;
    }

    return true;
}

void Document::serialize(autil::DataBuffer& dataBuffer) const
{
    uint32_t serializeVersion = GetSerializedVersion();
    dataBuffer.write(serializeVersion);
    DoSerialize(dataBuffer, serializeVersion);
}

void Document::deserialize(autil::DataBuffer& dataBuffer)
{
    dataBuffer.read(mSerializedVersion);
    DoDeserialize(dataBuffer, mSerializedVersion);
}

void Document::SetSerializedVersion(uint32_t version)
{
    uint32_t docVersion = GetDocBinaryVersion();
    if (version != INVALID_DOC_VERSION && version > docVersion) {
        INDEXLIB_THROW(util::BadParameterException, "target serializedVersion [%u] bigger than doc binary version [%u]",
                       version, docVersion);
    }
    mSerializedVersion = version;
}

uint32_t Document::GetSerializedVersion() const
{
    if (mSerializedVersion != INVALID_DOC_VERSION) {
        return mSerializedVersion;
    }
    mSerializedVersion = GetDocBinaryVersion();
    return mSerializedVersion;
}

const string& Document::GetPrimaryKey() const
{
    static string EMPTY_STRING = "";
    return EMPTY_STRING;
}

void Document::AddTag(const string& key, const string& value) { mTagInfo[key] = value; }

const string& Document::GetTag(const string& key) const
{
    auto iter = mTagInfo.find(key);
    if (iter != mTagInfo.end()) {
        return iter->second;
    }
    static string emptyString;
    return emptyString;
}

string Document::TagInfoToString() const
{
    if (mTagInfo.empty()) {
        return string("");
    }
    if (mTagInfo.size() == 1 && mTagInfo.find(DOCUMENT_SOURCE_TAG_KEY) != mTagInfo.end()) {
        // legacy format for mSource
        return GetTag(DOCUMENT_SOURCE_TAG_KEY);
    }
    return autil::legacy::FastToJsonString(mTagInfo, true);
}

void Document::TagInfoFromString(const string& str)
{
    mTagInfo.clear();
    if (str.empty()) {
        return;
    }
    if (*str.begin() == '{') {
        if (ENABLE_SIMPLE_TAG_INFO_DECODE) {
            SimpleTagInfoDecode(str, &mTagInfo);
        } else {
            autil::legacy::FastFromJsonString(mTagInfo, str);
        }
        return;
    }
    AddTag(DOCUMENT_SOURCE_TAG_KEY, str);
}

void Document::SimpleTagInfoDecode(const std::string& str, TagInfoMap* tagInfo)
{
    size_t sz = str.size();
    size_t start = 0;
    size_t end = 0;
    bool inString = false;
    std::string key;
    std::string value;
    for (size_t i = 0; i < sz; ++i) {
        char c = str[i];
        if (c == ':') {
            key = str.substr(start, end - start);
        } else if (c == ',' or c == '}') {
            value = str.substr(start, end - start);
            (*tagInfo)[std::move(key)] = std::move(value);
            key.clear();
            value.clear();
        } else if (c == '"') {
            if (!inString) {
                start = i + 1;
            } else {
                end = i;
            }
            inString = !inString;
        }
    }
}

bool Document::IsUserDoc() const
{
    return mOpType == UNKNOWN_OP || mOpType == ADD_DOC || mOpType == DELETE_DOC || mOpType == UPDATE_FIELD ||
           mOpType == SKIP_DOC || mOpType == DELETE_SUB_DOC;
}

void Document::SetLocator(const indexlib::document::IndexLocator& indexLocator)
{
    mLocator.SetLocator(indexLocator.toString());
    mLocatorV2.SetSrc(indexLocator.getSrc());
    // may need to set concurrentIdx
    mLocatorV2.SetOffset({indexLocator.getOffset(), 0});
    // TODO(tianxiao.ttx) may need to test progress and offset suitable
    if (indexLocator.getProgress().size() > 0) {
        mLocatorV2.SetProgress(indexLocator.getProgress());
    }
}

static void TranslateLocator(const Locator& locator, indexlibv2::framework::Locator& locatorV2)
{
    indexlib::document::IndexLocator indexLocator;
    indexLocator.fromString(locator.GetLocator());
    locatorV2.SetSrc(indexLocator.getSrc());
    locatorV2.SetOffset({indexLocator.getOffset(), 0});
}

void Document::SetLocator(const indexlibv2::framework::Locator& locator)
{
    mLocatorV2 = locator;
    indexlib::document::IndexLocator indexLocator(locator.GetSrc(), locator.GetOffset().first, locator.GetUserData());
    mLocator.SetLocator(indexLocator.toString());
}

void Document::SetLocator(const Locator& locator)
{
    mLocator = locator;
    TranslateLocator(mLocator, mLocatorV2);
}

void Document::SetLocator(const std::string& locatorStr)
{
    mLocator.SetLocator(locatorStr);
    TranslateLocator(mLocator, mLocatorV2);
}

void Document::ToRawDocument(RawDocument* rawDoc)
{
    assert(rawDoc);
    rawDoc->setDocOperateType(mOpType);
    rawDoc->setDocTimestamp(mTimestamp);
    rawDoc->SetDocInfo(mDocInfo);
    for (const auto& [k, v] : mTagInfo) {
        rawDoc->AddTag(k, v);
    }
    rawDoc->SetLocator(mLocatorV2);
}

}} // namespace indexlib::document
