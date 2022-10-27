#include <ha3/sorter/SorterResource.h>

BEGIN_HA3_NAMESPACE(sorter);

SorterLocation transSorterLocation(const std::string &location) {
    if (location == "SL_SEARCHER") {
        return SL_SEARCHER;
    } else if (location == "SL_SEARCHCACHEMERGER") {
        return SL_SEARCHCACHEMERGER;
    } else if (location == "SL_PROXY") {
        return SL_PROXY;
    } else if (location == "SL_QRS" ) {
        return SL_QRS;
    } else {
        return SL_UNKNOWN;
    }
}

END_HA3_NAMESPACE(sorter);
