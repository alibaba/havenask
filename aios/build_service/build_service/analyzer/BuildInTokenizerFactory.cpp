#include "build_service/analyzer/BuildInTokenizerFactory.h"
#include "build_service/analyzer/AnalyzerDefine.h"
#include "build_service/analyzer/SimpleTokenizer.h"
#include "build_service/analyzer/SingleWSTokenizer.h"
#include "build_service/analyzer/PrefixTokenizer.h"
#include "build_service/analyzer/SuffixPrefixTokenizer.h"
#include "build_service/util/FileUtil.h"

using namespace std;

namespace build_service {
namespace analyzer {
BS_LOG_SETUP(analyzer, BuildInTokenizerFactory);

BuildInTokenizerFactory::BuildInTokenizerFactory() {
}

BuildInTokenizerFactory::~BuildInTokenizerFactory() {
}

void BuildInTokenizerFactory::destroy() {
}

bool BuildInTokenizerFactory::init(const KeyValueMap &parameters,
                                   const config::ResourceReaderPtr &resourceReader)
{
    return true;
}

Tokenizer* BuildInTokenizerFactory::createTokenizer(const string &tokenizerType) {
    Tokenizer * tokenizer = NULL;
    if (tokenizerType == SIMPLE_ANALYZER) {
        tokenizer = new SimpleTokenizer();
    } else if (tokenizerType == SINGLEWS_ANALYZER) {
        tokenizer = new SingleWSTokenizer();
    } else if (tokenizerType == PREFIX_ANALYZER) {
        tokenizer = new PrefixTokenizer();
    } else if (tokenizerType == SUFFIX_PREFIX_ANALYZER) {
        tokenizer = new SuffixPrefixTokenizer();
    } else {
        BS_LOG(ERROR, "unsupported analyzer %s", tokenizerType.c_str());
    }
    return tokenizer;
}

}
}
