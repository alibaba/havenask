#pragma once

#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/table/index_task/LocalTabletMergeController.h"

namespace indexlibv2 { namespace table {
class LocalTabletMergeControllerForTest : public LocalTabletMergeController
{
public:
    void TEST_SetSpecifyEpochId(std::string specifyEpochId) { _specifyEpochIdForTest = specifyEpochId; }
    void TEST_SetSpecifyMaxMergedSegIdAndVersionId(segmentid_t maxMergeSegId, versionid_t maxVersionId)
    {
        _specifyMaxSegmentId = maxMergeSegId;
        _specifyMaxVersionId = maxVersionId;
    }
    std::unique_ptr<framework::IndexTaskContext>
    CreateTaskContext(versionid_t baseVersionId, const std::string& taskType, const std::string& taskName,
                      const std::string& taskTraceId, const std::map<std::string, std::string>& params) override
    {
        auto context =
            LocalTabletMergeController::CreateTaskContext(baseVersionId, taskType, taskName, taskTraceId, params);
        if (!context) {
            return nullptr;
        }
        if (_specifyMaxSegmentId != -2) {
            context->TEST_SetSpecifyMaxMergedSegId(_specifyMaxSegmentId);
        }
        if (_specifyMaxVersionId != -2) {
            context->TEST_SetSpecifyMaxMergedVersionId(_specifyMaxVersionId);
        }
        for (auto extendResource : _extendResources) {
            auto status = context->GetResourceManager()->AddExtendResource(extendResource);
            if (!status.IsOK()) {
                return nullptr;
            }
        }
        return context;
    }
    void SetTaskResources(std::vector<std::shared_ptr<framework::IndexTaskResource>> extendResources)
    {
        _extendResources = std::move(extendResources);
    }
    std::string GetTaskPlan() { return _taskPlanStr; }
    void SetTaskPlan(const std::string& taskPlan) { _taskPlanStr = taskPlan; }

    future_lite::coro::Lazy<Status> SubmitMergeTask(std::unique_ptr<framework::IndexTaskPlan> plan,
                                                    framework::IndexTaskContext* context) override
    {
        if (_taskPlanStr.empty()) {
            co_return co_await LocalTabletMergeController::SubmitMergeTask(std::move(plan), context);
        } else {
            auto tmpPlan = std::make_unique<framework::IndexTaskPlan>("", "");
            FromJsonString(*tmpPlan, _taskPlanStr);
            co_return co_await LocalTabletMergeController::SubmitMergeTask(std::move(tmpPlan), context);
        }
    }

protected:
    std::string GenerateEpochId() const override
    {
        if (!_specifyEpochIdForTest.empty()) {
            return _specifyEpochIdForTest;
        }
        return LocalTabletMergeController::GenerateEpochId();
    }

private:
    std::string _specifyEpochIdForTest;
    segmentid_t _specifyMaxSegmentId = -2;
    versionid_t _specifyMaxVersionId = -2;
    std::string _taskPlanStr;
    std::vector<std::shared_ptr<framework::IndexTaskResource>> _extendResources;
};

}} // namespace indexlibv2::table
