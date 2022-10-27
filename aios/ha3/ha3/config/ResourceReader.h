#ifndef ISEARCH_RESOURCEREADER_H
#define ISEARCH_RESOURCEREADER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <suez/config/ResourceReader.h>

BEGIN_HA3_NAMESPACE(config);

typedef suez::ResourceReader ResourceReader;

HA3_TYPEDEF_PTR(ResourceReader);

END_HA3_NAMESPACE(config);

#endif //ISEARCH_RESOURCEREADER_H
