#include <temporal/log/logger.h>

#include <array>
#include <chrono>
#include <ctime>
#include <cstdio>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>

namespace temporal::log {

const char* LevelName(Level level) {
  switch (level) {
    case Level::Debug:
      return "DEBUG";
    case Level::Info:
      return "INFO";
    case Level::Warn:
      return "WARN";
    case Level::Error:
      return "ERROR";
  }
  return "?";
}

namespace {

std::string Utc8601() {
  using namespace std::chrono;
  const auto now = system_clock::now();
  const auto secs = system_clock::to_time_t(now);
  const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
  std::tm tm{};
  ::gmtime_r(&secs, &tm);
  std::array<char, 32> buf{};
  std::snprintf(buf.data(), buf.size(), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ", tm.tm_year + 1900,
                tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec,
                static_cast<int>(ms.count()));
  return std::string(buf.data());
}

// Writes one structured line per record to stderr, e.g.
//   2026-06-08T12:00:00.123Z INFO workflow completed run_id=abc result=ok
class ConsoleLogger final : public Logger {
 public:
  void Log(Level level, std::string_view message, const std::vector<Field>& fields) override {
    std::ostringstream os;
    os << Utc8601() << ' ' << LevelName(level) << ' ' << message;
    for (const auto& f : fields) {
      os << ' ' << f.key << '=' << f.value;
    }
    os << '\n';
    const std::lock_guard<std::mutex> lock(mu_);
    std::clog << os.str();
  }

 private:
  std::mutex mu_;
};

}  // namespace

std::shared_ptr<Logger> DefaultLogger() {
  static const std::shared_ptr<Logger> logger = std::make_shared<ConsoleLogger>();
  return logger;
}

}  // namespace temporal::log
