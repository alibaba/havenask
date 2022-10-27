#ifndef __INDEXLIB_MERGE_STRATEGY_CREATOR_H
#define __INDEXLIB_MERGE_STRATEGY_CREATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/merger/segment_directory.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/merger/merge_strategy/merge_strategy.h"
#include <string>
#include <tr1/memory>

IE_NAMESPACE_BEGIN(merger);

#define DECLARE_MERGE_STRATEGY_CREATOR(classname, identifier)           \
    std::string GetIdentifier() const override {return identifier;}    \
    class Creator : public MergeStrategyCreator                         \
    {                                                                   \
    public:                                                             \
        std::string GetIdentifier() const {return identifier;}          \
        MergeStrategyPtr Create(                                        \
                const SegmentDirectoryPtr &segDir,                      \
                const config::IndexPartitionSchemaPtr &schema) const    \
        {                                                               \
            return MergeStrategyPtr(new classname(segDir, schema));     \
        }                                                               \
    };

class MergeStrategyCreator
{
public:
    MergeStrategyCreator(){}
    virtual ~MergeStrategyCreator() {}

public:
    virtual std::string GetIdentifier() const = 0;
    virtual MergeStrategyPtr Create(
            const SegmentDirectoryPtr &segDir,
            const config::IndexPartitionSchemaPtr &schema) const = 0;
};

typedef std::tr1::shared_ptr<MergeStrategyCreator> MergeStrategyCreatorPtr;

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_STRATEGY_CREATOR_H
