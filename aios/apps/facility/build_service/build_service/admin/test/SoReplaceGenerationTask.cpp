#include "build_service/admin/test/SoReplaceGenerationTask.h"

using namespace std;

namespace build_service { namespace admin {

bool gGenerationTaskFake = false;

void setGenerationTaskToFake(bool delegate) { gGenerationTaskFake = delegate; }

void GenerationTask::stopGeneration()
{
    if (gGenerationTaskFake) {
        _step = GENERATION_STOPPED;
    } else {
        typedef void (*func_type)(GenerationTask*);
        func_type func = GET_NEXT_FUNC(func_type);
        func(this);
    }
}

void GenerationTaskBase::deleteIndex()
{
    if (gGenerationTaskFake) {
        _indexDeleted = true;
    } else {
        typedef void (*func_type)(GenerationTaskBase*);
        func_type func = GET_NEXT_FUNC(func_type);
        func(this);
    }
}

}} // namespace build_service::admin
