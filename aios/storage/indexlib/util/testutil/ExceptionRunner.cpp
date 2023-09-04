#include "indexlib/util/testutil/ExceptionRunner.h"

#include "gtest/gtest.h"
#include <random>

#include "fslib/fs/ExceptionTrigger.h"
#include "indexlib/util/Exception.h"

using namespace autil;
using namespace fslib::fs;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, ExceptionRunner);

using std::string;

void ExceptionUtilPrint(string output) { std::cout << output; }

Action::Action(std::function<void()> functor, int failureTypeCap) : _functor(functor), _failureTypeCap(failureTypeCap)
{
}

void Action::Success() const
{
    _functor();
    // ExceptionUtilPrint("s");
}

void Action::Failure() const
{
    ExceptionTrigger::InitTrigger(0, /*probabilityMode=*/true);
    try {
        _functor();
    } catch (const autil::legacy::ExceptionBase& e) {
        ExceptionTrigger::PauseTrigger();
    }
    // ExceptionUtilPrint("f");
    ExceptionTrigger::PauseTrigger();
}

int Action::GetFailureCap() const { return _failureTypeCap; }

Sequence::Sequence(const std::vector<Action>& actions) : _actions(actions.begin(), actions.end()) {}

void Sequence::Execute() const
{
    for (size_t i = 0; i < _actions.size(); ++i) {
        if (i != _actions.size() - 1) {
            _actions[i].Success();
        } else {
            _actions[i].Failure();
        }
    }
    // ExceptionUtilPrint("->");
}

void Sequence::AlwaysSucceed()
{
    for (size_t i = 0; i < _actions.size(); ++i) {
        _actions[i].Success();
    }
}

int Sequence::GetFailureCap() const { return _actions.back().GetFailureCap(); }

string Sequence::DebugString() const
{
    string seq = "";
    for (size_t i = 0; i < _actions.size() - 1; ++i) {
        seq = seq + "s";
    }
    return seq + "f";
}

std::map<int, std::vector<std::vector<Sequence>>>
ExceptionRunner::GenerateSequenceCombinations(int depth, const std::vector<Sequence>& sequences)
{
    // depth d -> combination at d map
    std::map<int, std::vector<std::vector<Sequence>>> combinations;
    for (int i = 1; i <= depth; ++i) {
        std::vector<std::vector<Sequence>> seqs;
        if (i == 1) {
            for (const auto& s : sequences) {
                seqs.push_back(std::vector<Sequence> {s});
            }
        } else {
            for (const auto& seq_prev_depth : combinations[i - 1]) {
                for (const auto& s : sequences) {
                    std::vector<Sequence> seq_cur_depth(seq_prev_depth.begin(), seq_prev_depth.end());
                    seq_cur_depth.push_back(s);
                    seqs.push_back(seq_cur_depth);
                }
            }
        }
        combinations[i] = seqs;
    }
    return combinations;
}

std::vector<Sequence> ExceptionRunner::GetSequenceFromActions()
{
    std::vector<Sequence> sequences;
    for (size_t i = 0; i < _actions.size(); ++i) {
        sequences.push_back(Sequence(std::vector<Action>(_actions.begin(), _actions.begin() + i + 1)));
    }
    return sequences;
}

string ExceptionRunner::GetSequencesDebugString(const std::vector<Sequence>& seqs)
{
    if (seqs.empty())
        return "";
    string res = seqs[0].DebugString();
    for (size_t i = 1; i < seqs.size(); ++i) {
        res = res + "->" + seqs[i].DebugString();
    }
    return res;
}

void ExceptionRunner::DisableLog()
{
    _logLevelIndexLib = alog::Logger::getLogger("indexlib")->getLevel();
    alog::Logger::getLogger("indexlib")->setLevel(alog::LOG_LEVEL_FATAL);
    _logLevelFsLib = alog::Logger::getLogger("fs")->getLevel();
    alog::Logger::getLogger("fs")->setLevel(alog::LOG_LEVEL_FATAL);
}

void ExceptionRunner::EnableLog()
{
    alog::Logger::getLogger("indexlib")->setLevel(_logLevelIndexLib);
    alog::Logger::getLogger("fs")->setLevel(_logLevelFsLib);
}

void ExceptionRunner::RunOnce(const std::vector<Sequence>& sequences, const std::vector<Sequence>& originalSequences)
{
    DisableLog();
    if (!sequences.empty()) {
        const Sequence& seq = sequences.front();
        for (int i = 0; i < seq.GetFailureCap(); ++i) {
            _reset();
            seq.Execute();
            RunOnce(std::vector<Sequence>(sequences.begin() + 1, sequences.end()), originalSequences);
        }
        return;
    }
    _reset();
    _successSequence.AlwaysSucceed();
    // ExceptionUtilPrint("\n");
    bool res = _verifier();
    EnableLog();
    if (!res) {
        AUTIL_LOG(ERROR, "Following sequences failed %s", GetSequencesDebugString(sequences).c_str());
        ASSERT_TRUE(res);
        ExceptionUtilPrint("debug string: " + GetSequencesDebugString(originalSequences) + "\n");
    }
}

void ExceptionRunner::Run()
{
    std::vector<Sequence> sequences = GetSequenceFromActions();
    std::map<int, std::vector<std::vector<Sequence>>> combinations =
        GenerateSequenceCombinations(_maxExceptionDepth, sequences);
    _successSequence = Sequence(std::vector<Action>(_actions.begin(), _actions.end()));
    for (const auto& pair : combinations) {
        for (const auto& comb : pair.second) {
            RunOnce(comb, comb);
        }
    }
}

// Used to debug a certain sequence.
void ExceptionRunner::RunOneSequence(int i)
{
    std::vector<Sequence> sequences = GetSequenceFromActions();
    std::map<int, std::vector<std::vector<Sequence>>> combinations =
        GenerateSequenceCombinations(_maxExceptionDepth, sequences);
    _successSequence = Sequence(std::vector<Action>(_actions.begin(), _actions.end()));
    int totalCount = 0;
    for (const auto& pair : combinations) {
        totalCount += pair.second.size();
    }
    INDEXLIB_FATAL_ERROR_IF(i < 0 || i >= totalCount, OutOfRange, "Invalid sequence number: %d", i);
    for (const auto& pair : combinations) {
        for (const auto& comb : pair.second) {
            if (i == 0) {
                ExceptionUtilPrint("debug string: " + GetSequencesDebugString(comb) + "\n");
                RunOnce(comb, comb);
            } else {
                i--;
            }
        }
    }
}
}} // namespace indexlib::util
