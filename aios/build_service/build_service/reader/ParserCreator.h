#ifndef ISEARCH_BS_PARSERCREATOR_H
#define ISEARCH_BS_PARSERCREATOR_H

#include <indexlib/document/document_factory_wrapper.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/document/raw_document_parser.h>

BS_DECLARE_REFERENCE_CLASS(proto, ParserConfig);

namespace build_service {
namespace reader {

class ParserCreator
{
public:
    ParserCreator(
        const IE_NAMESPACE(document)::DocumentFactoryWrapperPtr& wrapper =
        IE_NAMESPACE(document)::DocumentFactoryWrapperPtr())
        : _wrapper(wrapper)
    {};
    ~ParserCreator() {};
private:
    ParserCreator(const ParserCreator &);
    ParserCreator& operator=(const ParserCreator &);
public:
    bool createParserConfigs(const KeyValueMap& kvMap,
                             std::vector<proto::ParserConfig>& parserConfigs);
    IE_NAMESPACE(document)::RawDocumentParser* create(const KeyValueMap& kvMap);
    const std::string& getLastError() const {
        return _lastErrorStr;
    }
private:
    IE_NAMESPACE(document)::RawDocumentParser* doCreateParser(
        const std::vector<proto::ParserConfig>& parserConfigs);
    IE_NAMESPACE(document)::RawDocumentParser* createSingleParser(
        const proto::ParserConfig& parserConfig);
    bool createParserConfigForLegacyConfig(
        const KeyValueMap& kvMap, proto::ParserConfig& parserConfig);

private:
    std::string _lastErrorStr;
    IE_NAMESPACE(document)::DocumentFactoryWrapperPtr _wrapper;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ParserCreator);

}
}

#endif //ISEARCH_BS_PARSERCREATOR_H
