#ifndef __INDEXLIB_DICTIONARY_CREATOR_H
#define __INDEXLIB_DICTIONARY_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_reader.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_writer.h"
#include "indexlib/config/index_config.h"

IE_NAMESPACE_BEGIN(index);

class DictionaryCreator
{
public:
    DictionaryCreator();
    ~DictionaryCreator();
    
public:
    static DictionaryReader* CreateReader(const config::IndexConfigPtr& config);
    static DictionaryReader* CreateIntegrateReader(const config::IndexConfigPtr& config);
    
    static DictionaryWriter* CreateWriter(const config::IndexConfigPtr& config,
                                          autil::mem_pool::PoolBase* pool);
    
    static DictionaryReader* CreateDiskReader(const config::IndexConfigPtr& config);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DictionaryCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DICTIONARY_CREATOR_H
