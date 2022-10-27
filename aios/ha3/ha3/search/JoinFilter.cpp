#include <ha3/search/JoinFilter.h>

using namespace std;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, JoinFilter);

JoinFilter::JoinFilter(JoinDocIdConverterCreator *convertFactory, 
                       bool forceStrongJoin)
        : _convertFactory(convertFactory) 
        , _filteredCount(0)
        , _forceStrongJoin(forceStrongJoin)
{ 
}

JoinFilter::~JoinFilter() { 
}

bool JoinFilter::doPass(matchdoc::MatchDoc doc, bool isSub) {
    const auto &convertMap = _convertFactory->getConverterMap();
    auto iter = convertMap.begin();
    for (; iter != convertMap.end(); iter++) {
        JoinDocIdConverterBase *converter = iter->second;
        if (converter->isSubJoin() != isSub) {
            continue;
        }
        if ( !_forceStrongJoin && !converter->isStrongJoin()) {
            continue;
        }
        if (converter->convert(doc) ==  INVALID_DOCID) {
            _filteredCount++;
            return false;
        }
    }
    return true;
}

END_HA3_NAMESPACE(search);

