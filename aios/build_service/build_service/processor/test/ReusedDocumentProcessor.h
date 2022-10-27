#ifndef __BUILD_REUSEDDOCUMENTPROCESSOR_H
#define __BUILD_REUSEDDOCUMENTPROCESSOR_H

#include "build_service/processor/DocumentProcessor.h"
#include <tr1/memory>

namespace build_service {
namespace processor {

class ReusedDocumentProcessor : public PooledDocumentProcessor
{
public:
    ReusedDocumentProcessor()
        : counter(0)
        , _fieldName("need_process_fail")
    {}
    
    ReusedDocumentProcessor(const ReusedDocumentProcessor& other)
        : PooledDocumentProcessor(other)
        , counter(0)
        , _fieldName(other._fieldName)
    {}
        
    ~ReusedDocumentProcessor() {}

public:
    /* override */ bool process(const document::ExtendDocumentPtr &document)
    {
        if (document->getRawDocument()->getField(_fieldName) == "true") {
            return false;
        }
        ++counter;
        return true;
    }

    /* override */ void destroy()
    {
        delete this;
    }

    /* override */ DocumentProcessor* clone() {return new ReusedDocumentProcessor(*this);}
    
    /* override */ bool init(const KeyValueMap &kvMap,
                             const IE_NAMESPACE(config)::IndexPartitionSchemaPtr &schemaPtr,
                             config::ResourceReader *resourceReader)
    {
        KeyValueMap::const_iterator iter = kvMap.find("field_name");
        if (iter != kvMap.end()) {
            _fieldName = iter->second;
        }
        return true;
    }

    /* override */ std::string getDocumentProcessorName() const {return "ReusedDocumentProcessor";}

public:
    int32_t counter;
    std::string _fieldName;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ReusedDocumentProcessor);

}
}

#endif //ISEARCH_BS_REUSEDDOCUMENTPROCESSOR_H
