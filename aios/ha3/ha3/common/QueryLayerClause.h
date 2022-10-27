#ifndef ISEARCH_QUERYLAYERCLAUSE_H
#define ISEARCH_QUERYLAYERCLAUSE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/LayerDescription.h>
#include <ha3/common/ClauseBase.h>

BEGIN_HA3_NAMESPACE(common);

class QueryLayerClause : public ClauseBase
{
public:
    QueryLayerClause();
    ~QueryLayerClause();
private:
    QueryLayerClause(const QueryLayerClause &);
    QueryLayerClause& operator=(const QueryLayerClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    size_t getLayerCount() const {
        return _layerDescriptions.size();
    }
    LayerDescription *getLayerDescription(size_t pos) const;
    void addLayerDescription(LayerDescription *layerDesc);
private:
    std::vector<LayerDescription*> _layerDescriptions;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryLayerClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_QUERYLAYERCLAUSE_H
