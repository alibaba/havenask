#include "build_service/processor/test/EraseFieldProcessor.h"
#include <autil/StringTokenizer.h>

using namespace std;
using namespace autil;

using namespace build_service::document;

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, EraseFieldProcessor);

EraseFieldProcessor::EraseFieldProcessor() {
}

EraseFieldProcessor::~EraseFieldProcessor() {
}

bool EraseFieldProcessor::process(const ExtendDocumentPtr &document) {
    const RawDocumentPtr &rawDoc = document->getRawDocument();
    for (size_t i = 0; i < _needEraseFieldNames.size(); ++i) {
        rawDoc->eraseField(_needEraseFieldNames[i]);
    }
    return true;
}

bool EraseFieldProcessor::init(const DocProcessorInitParam &param) {
    KeyValueMap::const_iterator it = param.parameters.find("erase_fields");
    if (it == param.parameters.end()) {
        return true;
    }
    _needEraseFieldNames = StringTokenizer::tokenize(ConstString(it->second), ",",
            StringTokenizer::TOKEN_IGNORE_EMPTY | StringTokenizer::TOKEN_TRIM);
    return true;
}

DocumentProcessor *EraseFieldProcessor::clone() {
    return new EraseFieldProcessor(*this);
}

}
}
