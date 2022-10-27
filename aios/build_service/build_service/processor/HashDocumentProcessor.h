#ifndef ISEARCH_BS_HASHDOCUMENTPROCESSOR_H
#define ISEARCH_BS_HASHDOCUMENTPROCESSOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/DocumentProcessor.h"
#include <autil/HashFunctionBase.h>
#include <indexlib/util/counter/counter_map.h>
#include "build_service/config/HashMode.h"
#include "build_service/config/HashModeConfig.h"

namespace build_service {
namespace processor {

class HashDocumentProcessor : public DocumentProcessor
{
public:
    static const std::string PROCESSOR_NAME;
public:
    HashDocumentProcessor();
    ~HashDocumentProcessor();
public:
    /* override */ bool init(const DocProcessorInitParam &param);
    /* override */ bool process(const document::ExtendDocumentPtr &document);
    /* override */ void batchProcess(const std::vector<document::ExtendDocumentPtr> &docs);
public:
    /* override */ void destroy();
    /* override */ DocumentProcessor* clone();
    /* override */ std::string getDocumentProcessorName() const {
        return PROCESSOR_NAME;
    }

private:
    struct ClusterHashMeta {
        std::string clusterName;
        std::vector<autil::HashFunctionBasePtr> hashFuncVec;
        std::vector<std::vector<std::string> > hashFieldVec;

        const autil::HashFunctionBasePtr& getHashFunc(regionid_t regionId) const
        {
            return hashFuncVec[regionId];
        }

        const std::vector<std::string>& getHashField(regionid_t regionId) const 
        {
            return hashFieldVec[regionId];
        }

        void appendHashInfo(autil::HashFunctionBasePtr hashFunc, const std::vector<std::string>& hashField)
        {
            hashFuncVec.push_back(hashFunc);
            hashFieldVec.push_back(hashField);
        }
    };

private:
    bool createClusterHashMeta(
            const config::HashModeConfig &hashModeConfig,
            ClusterHashMeta &clusterHashMeta);    

private:
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;
    std::vector<std::string> _clusterNames;
    std::vector<ClusterHashMeta> _clusterHashMetas;
    IE_NAMESPACE(util)::AccumulativeCounterPtr _hashFieldErrorCounter;    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(HashDocumentProcessor);

}
}

#endif //ISEARCH_BS_HASHDOCUMENTPROCESSOR_H
