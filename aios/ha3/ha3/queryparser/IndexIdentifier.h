#ifndef ISEARCH_INDEXIDENTIFIER_H
#define ISEARCH_INDEXIDENTIFIER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/isearch.h>

BEGIN_HA3_NAMESPACE(queryparser);

struct IndexIdentifier {
    std::string indexName;
    IndexIdentifier(const std::string &indexName) {
        this->indexName = indexName;
    }
};

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_INDEXIDENTIFIER_H
