#ifndef ISEARCH_LAYERMETASCONSTRUCTOR_H
#define ISEARCH_LAYERMETASCONSTRUCTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/LayerMetas.h>
BEGIN_HA3_NAMESPACE(search);

class LayerMetasConstructor
{
public:
    LayerMetasConstructor();
    ~LayerMetasConstructor();
private:
    LayerMetasConstructor(const LayerMetasConstructor &);
    LayerMetasConstructor& operator=(const LayerMetasConstructor &);
public:
    static LayerMeta createLayerMeta(autil::mem_pool::Pool *pool,
            const std::string &layerMetaStr);
    static LayerMetas createLayerMetas(autil::mem_pool::Pool *pool,
            const std::string &layerMetasStr);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(LayerMetasConstructor);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_LAYERMETASCONSTRUCTOR_H
