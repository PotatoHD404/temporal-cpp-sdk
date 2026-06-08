#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "internal/determinism.h"

namespace {

using temporal::internal::CommandEvent;
using temporal::internal::MatchReplayCommands;
using Kind = CommandEvent::Kind;

CommandEvent Act(std::string id, std::string type) {
  return {Kind::Activity, std::move(id), std::move(type)};
}
CommandEvent Timer(std::string id) { return {Kind::Timer, std::move(id), ""}; }
CommandEvent Child(std::string id, std::string type) {
  return {Kind::ChildWorkflow, std::move(id), std::move(type)};
}
CommandEvent Complete() { return {Kind::CompleteWorkflow, "", ""}; }

TEST(Determinism, IdenticalStreamsMatch) {
  std::vector<CommandEvent> h = {Act("0", "A"), Timer("t0"), Complete()};
  std::vector<CommandEvent> p = {Act("0", "A"), Timer("t0"), Complete()};
  EXPECT_FALSE(MatchReplayCommands(p, h).has_value());
}

TEST(Determinism, EmptyHistoryAlwaysMatches) {
  // First task of a fresh workflow: history has no command events yet.
  std::vector<CommandEvent> p = {Act("0", "A"), Complete()};
  EXPECT_FALSE(MatchReplayCommands(p, {}).has_value());
}

TEST(Determinism, ExtraTrailingProducedCommandsAreAllowed) {
  // The workflow has advanced past the replayed history (genuine forward progress).
  std::vector<CommandEvent> h = {Act("0", "A")};
  std::vector<CommandEvent> p = {Act("0", "A"), Timer("t0"), Act("1", "B")};
  EXPECT_FALSE(MatchReplayCommands(p, h).has_value());
}

TEST(Determinism, MissingReplayCommandIsDetected) {
  // History recorded two activities; replay produced only one -> divergence.
  std::vector<CommandEvent> h = {Act("0", "A"), Act("1", "B")};
  std::vector<CommandEvent> p = {Act("0", "A")};
  const auto err = MatchReplayCommands(p, h);
  ASSERT_TRUE(err.has_value());
  EXPECT_NE(err->find("missing replay command"), std::string::npos);
}

TEST(Determinism, ActivityTypeMismatchIsDetected) {
  std::vector<CommandEvent> h = {Act("0", "A")};
  std::vector<CommandEvent> p = {Act("0", "B")};
  const auto err = MatchReplayCommands(p, h);
  ASSERT_TRUE(err.has_value());
  EXPECT_NE(err->find("nondeterministic"), std::string::npos);
}

TEST(Determinism, ActivityIdMismatchIsDetected) {
  std::vector<CommandEvent> h = {Act("0", "A")};
  std::vector<CommandEvent> p = {Act("1", "A")};
  EXPECT_TRUE(MatchReplayCommands(p, h).has_value());
}

TEST(Determinism, CommandKindSwapIsDetected) {
  // History recorded an activity where the workflow now starts a timer.
  std::vector<CommandEvent> h = {Act("0", "A")};
  std::vector<CommandEvent> p = {Timer("t0")};
  EXPECT_TRUE(MatchReplayCommands(p, h).has_value());
}

TEST(Determinism, OrderSwapIsDetected) {
  std::vector<CommandEvent> h = {Act("0", "A"), Timer("t0")};
  std::vector<CommandEvent> p = {Timer("t0"), Act("0", "A")};
  EXPECT_TRUE(MatchReplayCommands(p, h).has_value());
}

TEST(Determinism, ChildWorkflowMatchesById) {
  std::vector<CommandEvent> h = {Child("wf_c0", "Child")};
  const std::vector<CommandEvent> same = {Child("wf_c0", "Child")};
  const std::vector<CommandEvent> diff_type = {Child("wf_c0", "Other")};
  const std::vector<CommandEvent> diff_id = {Child("wf_c9", "Child")};
  EXPECT_FALSE(MatchReplayCommands(same, h).has_value());
  EXPECT_TRUE(MatchReplayCommands(diff_type, h).has_value());
  EXPECT_TRUE(MatchReplayCommands(diff_id, h).has_value());
}

TEST(Determinism, TerminalKindMustMatch) {
  // History completed; replay tries to fail (or vice versa).
  std::vector<CommandEvent> completed = {Act("0", "A"), Complete()};
  std::vector<CommandEvent> failed = {Act("0", "A"), {Kind::FailWorkflow, "", ""}};
  EXPECT_TRUE(MatchReplayCommands(failed, completed).has_value());
  EXPECT_FALSE(MatchReplayCommands(completed, completed).has_value());
}

}  // namespace
