#ifndef ISEARCH_BS_BINARYDOCUMENTPROCESSOR_H
#define ISEARCH_BS_BINARYDOCUMENTPROCESSOR_H

#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"
#include <indexlib/indexlib.h>
#include <indexlib/config/index_partition_schema.h>

namespace build_service {
namespace processor {

class BinaryDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
    static const std::string BINARY_FIELD_NAMES;
    static const std::string FIELD_NAME_SEP;
public:
    BinaryDocumentProcessor();
    ~BinaryDocumentProcessor();
public:
    virtual bool process(const document::ExtendDocumentPtr &document);
    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    virtual void destroy();
    virtual DocumentProcessor* clone();
    virtual bool init(const KeyValueMap &kvMap,
                      const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                      config::ResourceReader *resourceReader);

    virtual std::string getDocumentProcessorName() const {return PROCESSOR_NAME;}

private:
    void processBinaryField(const document::RawDocumentPtr &rawDoc,
                            const std::string &fieldName);
    bool base64Decode(const std::string &input,
                      std::string &output);

private:
    typedef std::vector<std::string> FieldNameVector;
    FieldNameVector _binaryFieldNames;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BinaryDocumentProcessor);

}
}

#endif //ISEARCH_BS_BINARYDOCUMENTPROCESSOR_H
