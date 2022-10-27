#ifndef ISEARCH_BS_MULTIVALUEFIELDPROCESSOR_H
#define ISEARCH_BS_MULTIVALUEFIELDPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class MultiValueFieldProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
public:
    MultiValueFieldProcessor();
    ~MultiValueFieldProcessor();
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
    std::map<std::string, std::string> _fieldSepDescription;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiValueFieldProcessor);

}
}

#endif //ISEARCH_BS_MULTIVALUEFIELDPROCESSOR_H
