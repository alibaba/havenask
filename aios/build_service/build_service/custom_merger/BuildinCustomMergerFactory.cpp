#include "build_service/custom_merger/BuildinCustomMergerFactory.h"
#include "build_service/custom_merger/AlterDefaultFieldMerger.h"
#include "build_service/custom_merger/ReuseAttributeAlterFieldMerger.h"

using namespace std;

namespace build_service {
namespace custom_merger {
BS_LOG_SETUP(custom_merger, BuildinCustomMergerFactory);

BuildinCustomMergerFactory::BuildinCustomMergerFactory() {
}

BuildinCustomMergerFactory::~BuildinCustomMergerFactory() {
}

CustomMerger* BuildinCustomMergerFactory::createCustomMerger(const std::string &mergerName) 
{
#define ENUM_MERGER(merger)                             \
    if (mergerName == merger::MERGER_NAME) {            \
        return new merger;                              \
    }

    ENUM_MERGER(AlterDefaultFieldMerger);
    ENUM_MERGER(ReuseAttributeAlterFieldMerger);    
    return NULL;
}

}
}
