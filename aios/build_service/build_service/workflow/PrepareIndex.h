#ifndef ISEARCH_BS_PREPAREINDEX_H
#define ISEARCH_BS_PREPAREINDEX_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/document/RawDocumentHashMapManager.h"
#include "build_service/document/RawDocument.h"

namespace build_service {
namespace workflow {

class PrepareIndex
{
public:
    PrepareIndex(const std::string &outputDir,
                 const std::string &configPath,
                 const std::string &clusterName,
                 int32_t generationId = 0,
                 uint16_t from = 0,
                 uint16_t to = 65535,
                 const std::string &dataTable = "simple");
    ~PrepareIndex();
private:
    PrepareIndex(const PrepareIndex &);
    PrepareIndex& operator=(const PrepareIndex &);
public:
    bool buildOneDocument();
    bool buildOneDocument(const std::map<std::string, std::string> &doc);
    bool buildDocuments(const std::vector<std::map<std::string, std::string> > &docs);
    bool buildDocuments(const std::vector<std::tr1::shared_ptr<document::RawDocument> > &docs);
    bool makeEmptyVersion();
    void setRange(uint16_t from, uint16_t to) {
        _from = from;
        _to = to;
    }
private:
    static std::tr1::shared_ptr<document::RawDocument> prepareDoc(const std::map<std::string, std::string> &doc);
    static std::tr1::shared_ptr<document::RawDocument> prepareDoc();
private:
    std::string _outputDir;
    std::string _configPath;
    std::string _clusterName;
    std::string _dataTable;
    int32_t _generationId;
    uint16_t _from;
    uint16_t _to;
    static document::RawDocumentHashMapManagerPtr _hashMapManager;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_PREPAREINDEX_H
