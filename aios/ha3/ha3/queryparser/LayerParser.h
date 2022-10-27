#ifndef ISEARCH_LAYERPARSER_H
#define ISEARCH_LAYERPARSER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/QueryLayerClause.h>

BEGIN_HA3_NAMESPACE(queryparser);

class LayerParser
{
public:
    LayerParser();
    ~LayerParser();
private:
    LayerParser(const LayerParser &);
    LayerParser& operator=(const LayerParser &);
public:
    common::QueryLayerClause *createLayerClause();
    common::LayerDescription *createLayerDescription();
    void setQuota(common::LayerDescription *layerDesc, const std::string &quota);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LayerParser);

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_LAYERPARSER_H
