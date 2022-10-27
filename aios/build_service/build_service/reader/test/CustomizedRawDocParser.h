#ifndef ISEARCH_READER_TEST_CUSTOMIZED_RAW_DOC_PARSER_H
#define ISEARCH_READER_TEST_CUSTOMIZED_RAW_DOC_PARSER_H

#include <build_service/config/ResourceReader.h>
#include <autil/StringUtil.h>
#include <indexlib/indexlib.h>
#include <indexlib/misc/log.h>
#include <indexlib/misc/common.h>
#include <indexlib/document/document_factory.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/document/document.h>
#include <indexlib/document/extend_document/indexlib_extend_document.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;

namespace build_service {
namespace reader {

class MyRawDocument : public IE_NAMESPACE(document)::RawDocument
{
public:
    MyRawDocument();
    ~MyRawDocument();
public:
    bool Init(const IE_NAMESPACE(document)::DocumentInitParamPtr&
              initParam) override
    { return true; }

    void setField(const autil::ConstString &fieldName,
                  const autil::ConstString &fieldValue) override;
    
    void setFieldNoCopy(const autil::ConstString &fieldName,
                        const autil::ConstString &fieldValue) override
    { setField(fieldName, fieldValue); }

    const autil::ConstString &getField(
        const autil::ConstString &fieldName) const override;

    bool exist(const autil::ConstString &fieldName) const override
    { return false; }

    void eraseField(const autil::ConstString &fieldName) override
    {}

    uint32_t getFieldCount() const override
    { return 1; }

    void clear() override {}

    IE_NAMESPACE(document)::RawDocument *clone() const override;
    IE_NAMESPACE(document)::RawDocument *createNewDocument() const override
    { return clone(); }
    std::string getIdentifier() const override
    { return 0; }
    uint32_t GetDocBinaryVersion() const override
    { return 0; }
    std::string toString() const override;
    
    void setData(const float* data);
    float* getData() { return mData; }
    size_t EstimateMemory() const override { return sizeof(*this); }
    
private:
    float mData[4];
private:
    static ConstString EMPTY_STRING;
};

typedef std::tr1::shared_ptr<MyRawDocument> MyRawDocumentPtr;

class MyRawDocumentParser : public IE_NAMESPACE(document)::RawDocumentParser
{
public:
    bool parse(const std::string &docString,
               IE_NAMESPACE(document)::RawDocument &rawDoc) override;
    bool parseMultiMessage(
        const std::vector<IE_NAMESPACE(document)::RawDocumentParser::Message>& msgs,
        IE_NAMESPACE(document)::RawDocument& rawDoc) override {
        assert(false);
        return false;
    }        
};

class MyDocumentFactory : public IE_NAMESPACE(document)::DocumentFactory
{
public:
    MyDocumentFactory() {}
    ~MyDocumentFactory() {}
public:
    IE_NAMESPACE(document)::RawDocument* CreateRawDocument(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema) override
    { return new MyRawDocument(); }
    
    IE_NAMESPACE(document)::DocumentParser* CreateDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema) override
    { return NULL; }
    
    IE_NAMESPACE(document)::RawDocumentParser* CreateRawDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
        const IE_NAMESPACE(document)::DocumentInitParamPtr& initParam) override
    { return new MyRawDocumentParser; }
private:
    IE_NAMESPACE(document)::KeyMapManagerPtr mHashKeyMapManager;
};


}
}
#endif //ISEARCH_READER_TEST_CUSTOMIZED_RAW_DOC_PARSER_H
