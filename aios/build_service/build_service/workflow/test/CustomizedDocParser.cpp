#include "CustomizedDocParser.h"
#include <sstream>
#include <autil/StringUtil.h>
#include "build_service/document/DocumentDefine.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(config);

namespace build_service {
namespace workflow {

void MyDocument::DoSerialize(autil::DataBuffer &dataBuffer,
                             uint32_t serializedVersion) const
{
    dataBuffer.write(mDocId);
    if (serializedVersion == GetDocBinaryVersion()) {
        dataBuffer.write(mData);
    } else {
        stringstream ss;
        ss << mData << "";
        string dataStr = ss.str();
        dataBuffer.write(dataStr);
    }
}
    
void MyDocument::DoDeserialize(autil::DataBuffer &dataBuffer,
                               uint32_t serializedVersion)
{
    dataBuffer.read(mDocId);
    if (serializedVersion == GetDocBinaryVersion()) {
        dataBuffer.read(mData);
    } else {
        string dataStr; 
        dataBuffer.read(dataStr);
        StringUtil::strToInt64(dataStr.c_str(), mData);
    }
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
    string dataStr = rawDoc->getField("price2");

    docid_t realId = -1;
    if (!StringUtil::strToInt32(myId.c_str(), realId))
    {
        return DocumentPtr();
    }

    MyDocument* document = new MyDocument();
    document->SetDocId(realId);
    document->SetDocOperateType(DocOperateType::ADD_DOC);
    int64_t data = -1;
    if (!StringUtil::strToInt64(dataStr.c_str(), data))
    {
        return DocumentPtr();
    }
    document->SetData(data);
    return DocumentPtr(document);
}

DocumentPtr MyDocumentParser::Parse(const ConstString& serializedData)
{
    DataBuffer dataBuffer(const_cast<char*>(serializedData.data()),
                          serializedData.length());
    MyDocumentPtr document;
    dataBuffer.read(document);
    return document;
}        
    
extern "C"
ModuleFactory* createDocumentFactory() {
    return new build_service::workflow::MyDocumentFactory;
}

extern "C"
void destroyDocumentFactory(ModuleFactory *factory) {
    factory->destroy();
}

}
}
