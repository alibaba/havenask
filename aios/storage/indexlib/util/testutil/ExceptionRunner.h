#pragma once

#include <functional>
#include <map>
#include <string>

#include "autil/Log.h"

namespace indexlib { namespace util {

// If there are n actions, the program completes correctly after all n actions are done successfully. Each action can be
// success(s) or failure(f). There will be n type of failure sequences.
// Example, n=4
// f, sf, ssf, sssf.
// The depth indicate the number of sequences above before the program completes. In theory, the program can always fail
// with one of the sequences above and restart from beginning and never completes.
// Example, for max depth=3, action number = 2, Possible combinations are:
// f->f->ss, f->sf->ss, sf->f->ss, sf->sf->ss, f->ss, sf->ss, ss
// Also, for each action, it might fail internally at multiple places, so its internal state might be different. For
// example, a file write might fail due to already exists or IO error. We don't distinguish these internal failures but
// limit its possibility to failureTypeCap. For example if action 1's failureTypeCap=2, action 2's failureTypeCap=3,
// f->sf->ss can run 6 times at most, but how it actually fails are determined randomly by ExceptionTrigger with random
// step.
// The total number of runs for maxExceptionDepth(d), n(number of actions), t(average number of failure types):
// (n^d)*(t^n).
// Be aware of the exponential growth. Also optimize to generate test sequences on the fly instead of all at once during
// initialiation.

class Action
{
public:
    Action(std::function<void()> functor, int failureTypeCap = 1);
    ~Action() {}

public:
    void Success() const;
    void Failure() const;
    int GetFailureCap() const;

private:
    // For complex actions, failureTypeCap can be a bigger value, for simple actions, failureTypeCap can be 1.
    std::function<void()> _functor;
    int _failureTypeCap = 1;
};

class Sequence
{
public:
    Sequence(const std::vector<Action>& actions);
    Sequence() {}

public:
    // Assume only the last action in the sequence can fail.
    void Execute() const;

    void AlwaysSucceed();
    std::string DebugString() const;
    int GetFailureCap() const;

private:
    std::vector<Action> _actions;
};

class ExceptionRunner
{
public:
    ExceptionRunner(int depth) : _maxExceptionDepth(depth) {}
    ~ExceptionRunner() {}
    void AddAction(const Action& action) { _actions.push_back(action); }
    void AddVerification(std::function<bool()> verifier) { _verifier = verifier; }
    void AddReset(std::function<void()> reset) { _reset = reset; }
    std::string GetSequencesDebugString(const std::vector<Sequence>& seqs);
    void Run();
    void RunOneSequence(int i);

private:
    void RunOnce(const std::vector<Sequence>& sequences, const std::vector<Sequence>& originalSequences);
    void DisableLog();
    void EnableLog();
    std::vector<Sequence> GetSequenceFromActions();
    std::map<int, std::vector<std::vector<Sequence>>>
    GenerateSequenceCombinations(int depth, const std::vector<Sequence>& sequences);

private:
    AUTIL_LOG_DECLARE();

private:
    std::vector<Action> _actions;
    Sequence _successSequence;
    std::function<void()> _reset;
    std::function<bool()> _verifier;
    int _maxExceptionDepth = 3;
    uint32_t _logLevelIndexLib;
    uint32_t _logLevelFsLib;
};
}} // namespace indexlib::util
