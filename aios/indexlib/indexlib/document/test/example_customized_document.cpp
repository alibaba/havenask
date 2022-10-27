#include "example_customized_document.h"
#include <autil/StringUtil.h>
#include <indexlib/plugin/ModuleFactory.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(config);

namespace indexlib_example
{

MyRawDocumentParser::MyRawDocumentParser()
{
    mId = "0";
}

void MyDocument::DoSerialize(autil::DataBuffer &dataBuffer,
                             uint32_t serializedVersion) const
{
    dataBuffer.write(mDocId);
}
    
void MyDocument::DoDeserialize(autil::DataBuffer &dataBuffer,
                               uint32_t serializedVersion)
{
    dataBuffer.read(mDocId);
}

void MyDocument::SetDocId(docid_t docId)
{
    mDocId = docId;
}

docid_t MyDocument::GetDocId() const
{
    return mDocId;
}

uint32_t MyDocument::GetDocBinaryVersion() const
{
    return 10;
}
    
MyDocumentParser::MyDocumentParser(const IndexPartitionSchemaPtr& schema)
    : DocumentParser(schema)
{
    cout << "construct MyDocumentParser" << endl;
}

MyDocumentParser::~MyDocumentParser()
{
    cout << "deconstruct MyDocumentParser" << endl;
}

bool MyDocumentParser::DoInit()
{
    return true;
}

DocumentPtr MyDocumentParser::Parse(const IndexlibExtendDocumentPtr& extendDoc)
{
    const RawDocumentPtr& rawDoc = extendDoc->getRawDocument();
    string myId = rawDoc->getField("myId");

    docid_t realId = -1;
    if (!StringUtil::strToInt32(myId.c_str(), realId))
    {
        return DocumentPtr();
    }

    DocumentPtr document(new MyDocument());
    document->SetDocId(realId);
    return document;
}

DocumentPtr MyDocumentParser::Parse(const ConstString& serializedData)
{
    DataBuffer dataBuffer(const_cast<char*>(serializedData.data()),
                          serializedData.length());
    MyDocumentPtr document;
    dataBuffer.read(document);
    return document;
}        
    
MyDocumentFactory::MyDocumentFactory()
    : mHashKeyMapManager(new KeyMapManager())
{ }
    
RawDocument* MyDocumentFactory::CreateRawDocument(
    const IndexPartitionSchemaPtr& schema)
{
    return new (std::nothrow) DefaultRawDocument(mHashKeyMapManager);  
}
    
DocumentParser* MyDocumentFactory::CreateDocumentParser(
    const IndexPartitionSchemaPtr& schema)
{
    return new (std::nothrow) MyDocumentParser(schema);
}

RawDocumentParser* MyDocumentFactory::CreateRawDocumentParser(
    const IndexPartitionSchemaPtr& schema,
    const DocumentInitParamPtr& initParam)
{
    return new MyRawDocumentParser();
}


extern "C"
ModuleFactory* createDocumentFactory() {
    return new indexlib_example::MyDocumentFactory;
}

extern "C"
void destroyDocumentFactory(ModuleFactory *factory) {
    factory->destroy();
}

}

