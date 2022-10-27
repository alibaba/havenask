#ifndef __INDEXLIB_EXAMPLE_CUSTOMIZED_DOCUMENT_H
#define __INDEXLIB_EXAMPLE_CUSTOMIZED_DOCUMENT_H

#include <tr1/memory>
#include <indexlib/indexlib.h>
#include <indexlib/common_define.h>
#include <indexlib/document/document_factory.h>
#include <indexlib/config/index_partition_schema.h>
#include <indexlib/document/document.h>
#include <indexlib/document/extend_document/indexlib_extend_document.h>
#include <indexlib/document/raw_document/default_raw_document.h>

namespace indexlib_example
{
class MyRawDocumentParser : public IE_NAMESPACE(document)::RawDocumentParser
{
public:
    bool parse(const std::string &docString,
               IE_NAMESPACE(document)::RawDocument &rawDoc) override
    {
        assert(false);
        return true;
    }
    bool parseMultiMessage(const std::vector<Message>& msgs, IE_NAMESPACE(document)::RawDocument& rawDoc)
    {
        assert(false);
        return true;
    }
};

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
    size_t EstimateMemory() const override;

private:
    docid_t mDocId;
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

class MyDocumentFactory : public IE_NAMESPACE(document)::DocumentFactory
{
public:
    MyDocumentFactory();
    ~MyDocumentFactory() {}
public:
    IE_NAMESPACE(document)::RawDocument* CreateRawDocument(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema) override;
    
    IE_NAMESPACE(document)::DocumentParser* CreateDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema) override;
    
    IE_NAMESPACE(document)::RawDocumentParser* CreateRawDocumentParser(
        const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema,
        const IE_NAMESPACE(document)::DocumentInitParamPtr& initParam) override;
private:
    IE_NAMESPACE(document)::KeyMapManagerPtr mHashKeyMapManager;
};

}

#endif //__INDEXLIB_EXAMPLE_CUSTOMIZED_DOCUMENT_H
