#ifndef ISEARCH_WORKFLOW_TEST_CUSTOMIZED_DOC_PARSER_H
#define ISEARCH_WORKFLOW_TEST_CUSTOMIZED_DOC_PARSER_H

#include <build_service/config/ResourceReader.h>
#include <autil/StringUtil.h>
#include <indexlib/indexlib.h>
#include <indexlib/misc/log.h>
#include <indexlib/misc/common.h>
#include <indexlib/misc/exception.h>
#include <indexlib/document/document_factory.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/document/document.h>
#include <indexlib/document/extend_document/indexlib_extend_document.h>
#include <indexlib/document/raw_document/default_raw_document.h>

using namespace std;
using namespace autil;

namespace build_service {
namespace workflow {

class MyDocument : public IE_NAMESPACE(document)::Document
{
protected:
    void DoSerialize(autil::DataBuffer &dataBuffer,
                     uint32_t serializedVersion) const override;
    
    void DoDeserialize(autil::DataBuffer &dataBuffer,
                       uint32_t serializedVersion) override;
public:
    uint32_t GetDocBinaryVersion() const override;
    void SetDocId(docid_t docId) override;
    docid_t GetDocId() const override;
    void SetData(int64_t data) { mData = data; }
    int64_t GetData() const { return mData; }
    size_t EstimateMemory() const override { return sizeof(*this); }
    
private:
    docid_t mDocId;
    int64_t mData;
};

typedef std::tr1::shared_ptr<MyDocument> MyDocumentPtr;

class MyDocumentParser : public IE_NAMESPACE(document)::DocumentParser
{
public:
    MyDocumentParser(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema);
    ~MyDocumentParser();
public:
    bool DoInit() override;
    IE_NAMESPACE(document)::DocumentPtr
        Parse(const IE_NAMESPACE(document)::IndexlibExtendDocumentPtr& extendDoc) override;
    IE_NAMESPACE(document)::DocumentPtr
        Parse(const autil::ConstString& serializedData) override;
};

typedef std::tr1::shared_ptr<MyDocumentParser> MyDocumentParserPtr;

class MyDocumentFactory : public IE_NAMESPACE(document)::DocumentFactory
{
public:
    MyDocumentFactory() {}
    ~MyDocumentFactory() {}
public:
    IE_NAMESPACE(document)::RawDocument* CreateRawDocument(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema) override
    { return NULL; }

    IE_NAMESPACE(document)::DocumentParser* CreateDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema) override
    { return new MyDocumentParser(schema); }
    
    IE_NAMESPACE(document)::RawDocumentParser* CreateRawDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
        const IE_NAMESPACE(document)::DocumentInitParamPtr& initParam) override
    { return NULL; }
};


}
}

#endif //ISEARCH_WORKFLOW_TEST_CUSTOMIZED_DOC_PARSER_H
