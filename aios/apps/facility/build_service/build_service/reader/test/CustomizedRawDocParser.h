#pragma once

#include <assert.h>
#include <cstddef>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Span.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/RawDocumentDefine.h"
#include "indexlib/document/document.h"
#include "indexlib/document/document_factory.h"
#include "indexlib/document/document_init_param.h"
#include "indexlib/document/document_parser.h"
#include "indexlib/document/raw_doc_field_iterator.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/document/raw_document/key_map_manager.h"
#include "indexlib/document/raw_document/raw_document_define.h"
#include "indexlib/document/raw_document_parser.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/indexlib.h"

using namespace std;
using namespace autil;

namespace build_service { namespace reader {

class MyRawDocument : public indexlib::document::RawDocument, public indexlib::document::Document
{
public:
    MyRawDocument();
    virtual ~MyRawDocument();

public:
    bool Init(const indexlib::document::DocumentInitParamPtr& initParam) override { return true; }

    void setField(const autil::StringView& fieldName, const autil::StringView& fieldValue) override;

    void setFieldNoCopy(const autil::StringView& fieldName, const autil::StringView& fieldValue) override
    {
        setField(fieldName, fieldValue);
    }

    const autil::StringView& getField(const autil::StringView& fieldName) const override;

    bool exist(const autil::StringView& fieldName) const override { return false; }

    void eraseField(const autil::StringView& fieldName) override {}

    uint32_t getFieldCount() const override { return 1; }

    void clear() override {}

    indexlib::document::RawDocument* clone() const override;
    indexlib::document::RawDocument* createNewDocument() const override { return clone(); }
    std::string getIdentifier() const override { return 0; }
    uint32_t GetDocBinaryVersion() const override { return 0; }
    std::string toString() const override;

    void setData(const float* data);
    float* getData() { return mData; }
    size_t EstimateMemory() const override { return sizeof(*this); }
    indexlib::document::RawDocFieldIterator* CreateIterator() const override { return nullptr; }
    int64_t GetUserTimestamp() const override { return INVALID_TIMESTAMP; }
    DocOperateType getDocOperateType() const override
    {
        if (_opType != UNKNOWN_OP) {
            return _opType;
        }
        const StringView& cmd = getField(StringView(indexlib::document::CMD_TAG));
        const_cast<MyRawDocument*>(this)->_opType = RawDocument::getDocOperateType(cmd);
        return _opType;
    }
    void setDocOperateType(DocOperateType type) override { _opType = type; }
    void extractFieldNames(std::vector<autil::StringView>& fieldNames) const override { assert(0); }
    int64_t getDocTimestamp() const override { return _timestamp; }
    void setDocTimestamp(int64_t timestamp) override { _timestamp = timestamp; }
    void AddTag(const std::string& key, const std::string& value) override { _tagInfo[key] = value; }
    const std::string& GetTag(const std::string& key) const override
    {
        auto iter = _tagInfo.find(key);
        if (iter != _tagInfo.end()) {
            return iter->second;
        }
        static string emptyString;
        return emptyString;
    }
    const std::map<std::string, std::string>& GetTagInfoMap() const override { return _tagInfo; }
    const indexlibv2::framework::Locator& getLocator() const override { return _locator; }
    void SetLocator(const indexlibv2::framework::Locator& locator) override { _locator = locator; }
    void SetIngestionTimestamp(int64_t ingestionTimestamp) override { assert(0); }
    int64_t GetIngestionTimestamp() const override
    {
        assert(0);
        return {};
    }
    void SetDocInfo(const indexlibv2::framework::Locator::DocInfo& docInfo) override { _docInfo = docInfo; }
    indexlibv2::framework::Locator::DocInfo GetDocInfo() const override { return _docInfo; }
    void SetDocId(docid_t docId) override { assert(0); }
    void DoSerialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) const override { assert(0); }
    void DoDeserialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) override { assert(0); }

private:
    float mData[4];
    std::map<std::string, std::string> _tagInfo;
    DocOperateType _opType = UNKNOWN_OP;
    int64_t _timestamp = INVALID_TIMESTAMP;
    indexlibv2::framework::Locator _locator;
    indexlibv2::framework::Locator::DocInfo _docInfo;

private:
    static StringView EMPTY_STRING;
};

typedef std::shared_ptr<MyRawDocument> MyRawDocumentPtr;

class MyRawDocumentParser : public indexlib::document::RawDocumentParser
{
public:
    bool parse(const std::string& docString, indexlib::document::RawDocument& rawDoc) override;
    bool parseMultiMessage(const std::vector<indexlib::document::RawDocumentParser::Message>& msgs,
                           indexlib::document::RawDocument& rawDoc) override
    {
        assert(false);
        return false;
    }
};

class MyDocumentFactory : public indexlib::document::DocumentFactory
{
public:
    MyDocumentFactory() {}
    ~MyDocumentFactory() {}

public:
    indexlib::document::RawDocument* CreateRawDocument(const indexlib::config::IndexPartitionSchemaPtr& schema) override
    {
        return new MyRawDocument();
    }

    indexlib::document::DocumentParser*
    CreateDocumentParser(const indexlib::config::IndexPartitionSchemaPtr& schema) override
    {
        return NULL;
    }

    indexlib::document::RawDocumentParser*
    CreateRawDocumentParser(const indexlib::config::IndexPartitionSchemaPtr& schema,
                            const indexlib::document::DocumentInitParamPtr& initParam) override
    {
        return new MyRawDocumentParser;
    }

private:
    indexlib::document::KeyMapManagerPtr mHashKeyMapManager;
};

}} // namespace build_service::reader
