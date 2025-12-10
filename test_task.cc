#include <algorithm>
#include <cctype>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <unordered_map>
#include <vector>

// Linux headers
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

constexpr size_t CHUNK_SIZE = 64 * (1 << 20); // 64 MB

constexpr auto IsAlphaPred = [](unsigned char c) { return std::isalpha(c); };

constexpr auto ToLowerPred = [](unsigned char c) {
  return static_cast<char>(std::tolower(c));
};

std::string_view
ProcessRemainderWithData(std::string_view chunk_data,
                         std::unordered_map<std::string, size_t> &hash_map,
                         std::string &remainder) {
  auto chunk_begin = chunk_data.begin();
  if (!remainder.empty()) {
    auto remainder_word_start =
        std::find_if(remainder.begin(), remainder.end(), IsAlphaPred);
    if (remainder_word_start != remainder.end()) {
      chunk_begin =
          std::find_if_not(chunk_data.begin(), chunk_data.end(), IsAlphaPred);
      if (chunk_begin == chunk_data.end()) {
        remainder.append(chunk_data);
        return {};
      }
      std::string complete_word;
      complete_word.reserve(
          std::distance(remainder_word_start, remainder.end()) +
          std::distance(chunk_data.begin(), chunk_begin));
      std::transform(remainder_word_start, remainder.end(),
                     std::back_inserter(complete_word), ToLowerPred);
      std::transform(chunk_data.begin(), chunk_begin,
                     std::back_inserter(complete_word), ToLowerPred);
      ++hash_map[std::move(complete_word)];
    }
  }
  auto rbegin = std::make_reverse_iterator(chunk_data.end());
  auto rend = std::make_reverse_iterator(chunk_begin);
  auto last_not_alpha = std::find_if_not(rbegin, rend, IsAlphaPred);
  auto last_complete_word_end =
      last_not_alpha == rend ? chunk_begin : std::prev(last_not_alpha.base());
  remainder.assign(last_complete_word_end, chunk_data.end());
  return std::string_view(&(*chunk_begin),
                          std::distance(chunk_begin, last_complete_word_end));
}

void CountWordsInChunk(std::string_view chunk_data,
                       std::unordered_map<std::string, size_t> &hash_map,
                       std::string &remainder) {
  auto data_without_remainder =
      ProcessRemainderWithData(chunk_data, hash_map, remainder);

  if (data_without_remainder.empty()) {
    return;
  }
  auto curr_pos = data_without_remainder.begin();
  auto chunk_end = data_without_remainder.end();

  while (curr_pos != chunk_end) {
    curr_pos = std::find_if(curr_pos, chunk_end, IsAlphaPred);

    if (curr_pos == chunk_end)
      break;

    auto word_end = std::find_if_not(curr_pos, chunk_end, IsAlphaPred);

    std::string word;
    word.reserve(std::distance(curr_pos, word_end));
    std::transform(curr_pos, word_end, std::back_inserter(word), ToLowerPred);

    ++hash_map[std::move(word)];

    curr_pos = word_end;
  }
}

std::vector<std::pair<std::string, size_t>>
ConvertToSortedVector(std::unordered_map<std::string, size_t> map) {
  std::vector<std::pair<std::string, size_t>> vector;
  vector.reserve(map.size());
  for (auto &elem : map) {
    vector.emplace_back(std::move(elem.first), elem.second);
  }
  std::sort(vector.begin(), vector.end(), [](const auto &a, const auto &b) {
    if (a.second != b.second) {
      return a.second > b.second;
    }
    return a.first < b.first;
  });
  return vector;
}

void ValidatePath(const std::filesystem::path &path) {
  if (path.empty()) {
    throw std::runtime_error("Incorrect format of input path");
  }

  if (!std::filesystem::exists(path)) {
    throw std::runtime_error("Input path is not exist");
  }

  if (!std::filesystem::is_regular_file(path)) {
    throw std::runtime_error("Input file is not regular");
  }
}

int OpenFileForReading(const std::filesystem::path &path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd == -1) {
    throw std::runtime_error("Cannot open file " + path.string());
  }
  return fd;
}

void ProcessFileByChunks(int fd, size_t file_size,
                         std::unordered_map<std::string, size_t> &hash_map,
                         std::string &remainder) {
  for (size_t offset = 0; offset < file_size; offset += CHUNK_SIZE) {
    size_t current_chunk_size = std::min(CHUNK_SIZE, file_size - offset);

    void *mapped_addr =
        mmap(nullptr, current_chunk_size, PROT_READ, MAP_PRIVATE, fd, offset);

    if (mapped_addr == MAP_FAILED) {
      throw std::runtime_error("Error during mmap");
    }

    std::string_view data(static_cast<char *>(mapped_addr), current_chunk_size);

    CountWordsInChunk(data, hash_map, remainder);

    if (munmap(mapped_addr, current_chunk_size) == -1) {
      throw std::runtime_error("Error during munmap");
    }
  }
}

void ProcessLastRemainder(std::string &remainder,
                          std::unordered_map<std::string, size_t> &hash_map) {
  auto begin_word =
      std::find_if(remainder.begin(), remainder.end(), IsAlphaPred);
  if (begin_word == remainder.end()) {
    return;
  }
  auto end_word = std::find_if_not(begin_word, remainder.end(), IsAlphaPred);
  std::string complete_word;
  complete_word.reserve(std::distance(begin_word, end_word));
  std::transform(begin_word, end_word, std::back_inserter(complete_word),
                 ToLowerPred);
  ++hash_map[std::move(complete_word)];
}

void OpenAndWriteToFile(
    const std::vector<std::pair<std::string, size_t>> &vector,
    std::filesystem::path path) {
  if (path.empty()) {
    throw std::runtime_error("Incorrect format of output path");
  }

  std::ostringstream oss;
  for (const auto &e : vector) {
    oss << e.second << " " << e.first << '\n';
  }

  std::ofstream file_out(path.c_str(), std::ios::binary);
  file_out << oss.str();
}

void ProcessFile(std::string_view input_file, std::string_view output_file) {
  auto input_path = std::filesystem::path(input_file);
  std::unordered_map<std::string, size_t> map;
  std::string remainder;

  ValidatePath(input_path);

  size_t file_size = std::filesystem::file_size(input_path);

  int fd = OpenFileForReading(input_path);
  try {
    ProcessFileByChunks(fd, file_size, map, remainder);
    ProcessLastRemainder(remainder, map);
  } catch (...) {
    close(fd);
    throw;
  }
  close(fd);

  auto vector = ConvertToSortedVector(std::move(map));

  OpenAndWriteToFile(vector, std::filesystem::path(output_file));
}

int main(int argc, char *argv[]) {
  if (argc != 3) {
    std::cout
        << "You should launch ./program path/to/input/file path/to/output/file"
        << std::endl;
    return 1;
  }
  try {
#if withTimer
    auto start = std::chrono::high_resolution_clock::now();
#endif

    ProcessFile(argv[1], argv[2]);
#if withTimer
    auto end = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Время выполнения: " << duration.count() << " мс\n";
#endif
  } catch (std::exception &error) {
    std::cout << "Something gone bad: " << error.what() << std::endl;
  }
  return 0;
}