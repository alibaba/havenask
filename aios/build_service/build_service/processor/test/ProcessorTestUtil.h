#ifndef ISEARCH_BS_PROCESSORTESTUTIL_H
#define ISEARCH_BS_PROCESSORTESTUTIL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/document/ExtendDocument.h"
#include <indexlib/indexlib.h>

IE_NAMESPACE_USE(common);

namespace build_service {
namespace processor {

class ProcessorTestUtil
{
public:
    ProcessorTestUtil();
    ~ProcessorTestUtil();
private:
    ProcessorTestUtil(const ProcessorTestUtil &);
    ProcessorTestUtil& operator=(const ProcessorTestUtil &);
public:
    static document::ExtendDocumentPtr createExtendDoc(
        const std::string &fieldMap, regionid_t regionId = DEFAULT_REGIONID);
    static document::RawDocumentPtr createRawDocument(const std::string &fieldMap);
    static std::vector<document::RawDocumentPtr> createRawDocuments(
        const std::string &rawDocStr, const std::string& sep = "#");
private:
    BS_LOG_DECLARE();
};

}
}


#endif //ISEARCH_BS_PROCESSORTESTUTIL_H
