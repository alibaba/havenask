#ifndef __DISTRIBUTE_DOCUMENT_PROCESSOR_H
#define __DISTRIBUTE_DOCUMENT_PROCESSOR_H

#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"

namespace build_service {
namespace processor {

class DistributeDocumentProcessor: public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
    static const std::string DISTRIBUTE_FIELD_NAMES;
    static const std::string DISTRIBUTE_RULE;
    static const std::string DEFAULT_CLUSTERS;
    static const std::string OTHER_CLUSTERS;
    static const std::string EMPTY_CLUSTERS;
    static const std::string ALL_CLUSTER;
    static const std::string FIELD_NAME_SEP;
    static const std::string KEY_VALUE_SEP;
    static const std::string CLUSTERS_SEP;
public:
    DistributeDocumentProcessor()
        : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
    {
    }
    ~DistributeDocumentProcessor() {
        //do nothing
    }

    /* override */ bool init(const DocProcessorInitParam &param);
    /* override */ bool process(const document::ExtendDocumentPtr &document);
    virtual void destroy();
    DistributeDocumentProcessor* clone() {
        return new DistributeDocumentProcessor(*this);
    }
    virtual std::string getDocumentProcessorName() const {
        return PROCESSOR_NAME;
    }

    virtual void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
    
private:
    std::string _distributeFieldName;
    std::map<std::string, std::vector<std::string> > _rule;
    std::vector<std::string> _otherClusters;
    std::vector<std::string> _emptyClusters;
private:
    BS_LOG_DECLARE();
};

}
}

#endif 
