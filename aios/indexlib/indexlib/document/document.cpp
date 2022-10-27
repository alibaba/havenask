#include "indexlib/document/document.h"
#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(document);

Document::Document()
    : mOpType(UNKNOWN_OP)
    , mOriginalOpType(UNKNOWN_OP)
    , mSerializedVersion(INVALID_DOC_VERSION)
    , mTTL(0)
    , mTimestamp(INVALID_TIMESTAMP)
    , mTrace(false)
{
}

Document::Document(const Document &other)
    : mOpType(other.mOpType)
    , mOriginalOpType(other.mOriginalOpType)
    , mSerializedVersion(other.mSerializedVersion)
    , mTTL(other.mTTL)
    , mTimestamp(other.mTimestamp)
    , mTagInfo(other.mTagInfo)
    , mLocator(other.mLocator)
    , mTrace(other.mTrace)
{
}

Document::~Document()
{
}

bool Document::operator==(const Document& doc) const
{
    if (this == &doc)
    {
        return true;
    }

    if (GetSerializedVersion() != doc.GetSerializedVersion())
    {
        return false;
    }
    
    if (mTimestamp != doc.mTimestamp) { return false; }
    if (mTagInfo != doc.mTagInfo) { return false; }
    if (mLocator != doc.mLocator) { return false; }
    if (mOpType != doc.mOpType) { return false; }
    if (mTTL != doc.mTTL) { return false; }
    if (mOriginalOpType != doc.mOriginalOpType) { return false; }

    return true;
}

void Document::serialize(autil::DataBuffer &dataBuffer) const
{
    uint32_t serializeVersion = GetSerializedVersion();
    dataBuffer.write(serializeVersion);
    DoSerialize(dataBuffer, serializeVersion);
}

void Document::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(mSerializedVersion);
    DoDeserialize(dataBuffer, mSerializedVersion);
}

void Document::SetSerializedVersion(uint32_t version)
{
    uint32_t docVersion = GetDocBinaryVersion();
    if (version != INVALID_DOC_VERSION && version > docVersion)
    {
        INDEXLIB_THROW(misc::BadParameterException,
                       "target serializedVersion [%u] bigger than doc binary version [%u]",
                       version, docVersion);
    }
    mSerializedVersion = version;
}

uint32_t Document::GetSerializedVersion() const
{
    if (mSerializedVersion != INVALID_DOC_VERSION)
    {
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

void Document::AddTag(const string& key, const string& value)
{
    mTagInfo[key] = value;
}

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
        autil::legacy::FastFromJsonString(mTagInfo, str);
        return;
    }
    AddTag(DOCUMENT_SOURCE_TAG_KEY, str);
}

IE_NAMESPACE_END(document);

