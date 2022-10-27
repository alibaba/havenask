#ifndef ISEARCH_FAKEMULTIVALUEMAKER_H
#define ISEARCH_FAKEMULTIVALUEMAKER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/MultiValueType.h>
#include <autil/MultiValueCreator.h>

IE_NAMESPACE_BEGIN(index);

class MultiCharMaker {
public:
    MultiCharMaker() {}

    autil::MultiChar makeMultiChar(const std::string &str) {
        autil::MultiChar ret;
        ret.init(autil::MultiValueCreator::createMultiValueBuffer(
                        str.data(), str.size(), &mPool));
        return ret;
    }
public:
    autil::mem_pool::Pool mPool;
};

IE_NAMESPACE_END(index);

#endif //ISEARCH_FAKEMULTIVALUEMAKER_H
