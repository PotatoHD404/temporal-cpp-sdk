#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace temporal::log {

enum class Level : std::uint8_t { Debug, Info, Warn, Error };

// A structured log field. The SDK never logs via bare printf/cout; all output
// flows through a Logger so applications can route it however they like.
struct Field {
  std::string key;
  std::string value;
};

inline Field F(std::string key, std::string value) {
  return Field{std::move(key), std::move(value)};
}

class Logger {
 public:
  Logger() = default;
  virtual ~Logger() = default;
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger(Logger&&) = delete;
  Logger& operator=(Logger&&) = delete;

  virtual void Log(Level level, std::string_view message, const std::vector<Field>& fields) = 0;

  void Debug(std::string_view m, const std::vector<Field>& f = {}) { Log(Level::Debug, m, f); }
  void Info(std::string_view m, const std::vector<Field>& f = {}) { Log(Level::Info, m, f); }
  void Warn(std::string_view m, const std::vector<Field>& f = {}) { Log(Level::Warn, m, f); }
  void Error(std::string_view m, const std::vector<Field>& f = {}) { Log(Level::Error, m, f); }
};

// Process-wide default logger: writes one structured line per record to stderr.
std::shared_ptr<Logger> DefaultLogger();

const char* LevelName(Level level);

}  // namespace temporal::log
