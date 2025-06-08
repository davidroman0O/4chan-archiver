# 4archive - 4chan Thread Archiver

A comprehensive CLI tool for archiving and monitoring 4chan threads with controlled downloading, metadata tracking, and support for multiple boards.

## Features

- **Thread Monitoring**: Continuously monitor active threads and archive new posts/media in real-time
- **Multi-thread Support**: Archive or monitor multiple threads concurrently
- **Controlled Downloads**: Rate limiting and retry mechanisms to be respectful to servers
- **Metadata Tracking**: JSON metadata files track archiving progress and prevent duplicate downloads
- **Board Support**: Works with any 4chan board (pol, b, etc.)
- **Flexible Content Types**: Choose what to archive (posts, media, or both)
- **Resume Capability**: Skip already downloaded content on subsequent runs
- **Status Monitoring**: Check archiving progress and view archived thread information
- **Smart Error Detection**: Automatically detects deleted/archived threads and stops monitoring
- **Source Flexibility**: Support for both 4chan and archived.moe

## Installation

```bash
# Clone the repository
git clone https://github.com/davidroman0O/4chan-archiver
cd 4chan-archiver

# Build the application
go build -o 4archive

# Install globally (optional)
sudo cp 4archive /usr/local/bin/4archive

# Or install directly with Go
go install
```

## Usage

### Archive Threads (One-time)

Archive one or multiple threads:

```bash
# Archive a single thread from /pol/
4archive archive --board pol 123456789

# Archive multiple threads
4archive archive --board b 123456789 987654321 555666777

# Archive with specific content types
4archive archive --board pol --media --posts 123456789

# Archive to a specific directory
4archive archive --board pol --output /path/to/archive 123456789

# Control download behavior
4archive archive --board pol --rate-limit 2000 --concurrency 2 123456789
```

### Monitor Threads (Continuous)

**NEW**: Continuously monitor active threads for new posts and media:

```bash
# Monitor a single thread with default settings (check every 5 minutes)
4archive monitor --board b 123456789

# Monitor multiple threads simultaneously
4archive monitor --board pol 123456789 987654321 555444333

# Custom monitoring with faster updates
4archive monitor --board b 123456789 --interval 2m --stop-on-inactivity 30m

# Monitor with custom output directory
4archive monitor --board pol 123456789 --output ~/Pictures/.chans --verbose

# Long-term monitoring with time limits
4archive monitor --board b 123456789 987654321 --max-duration 6h --interval 1m

# Research monitoring (monitor until threads are archived)
4archive monitor --board pol 123456789 --stop-on-archive --verbose
```

The monitor automatically:
- **Validates threads exist** before starting monitoring
- **Downloads only new content** since the last check
- **Detects deleted/archived threads** and stops monitoring them
- **Handles interruptions gracefully** (can resume later)
- **Monitors multiple threads concurrently** with proper resource management

### Check Status

View archiving status and metadata:

```bash
# Check status of specific threads
4archive status --board pol 123456789 987654321

# List all archived threads for a board
4archive status --board pol --list

# Check status with verbose output
4archive status --board pol --verbose 123456789
```

## Configuration

### Command Line Flags

#### Global Flags
| Flag | Description | Default |
|------|-------------|---------|
| `--board, -b` | Board name (pol, b, etc.) | `pol` |
| `--output, -o` | Output directory | `~/Documents/4chan-archive` |
| `--rate-limit` | Rate limit between requests (ms) | `1000` |
| `--max-retries` | Maximum retries for failed requests | `3` |
| `--concurrency, -c` | Maximum concurrent downloads | `3` |
| `--user-agent` | Custom user agent | Random |
| `--verbose, -v` | Enable verbose logging | `false` |

#### Archive/Monitor Content Flags
| Flag | Description | Default |
|------|-------------|---------|
| `--content` | Include thread content/posts | `true` |
| `--media` | Include media files | `true` |
| `--posts` | Include post text and metadata | `true` |
| `--skip-existing` | Skip files that already exist | `true` |
| `--source` | Source: '4chan', 'archived.moe', or 'auto' | `auto` |
| `--database-mode` | Database mode: 'memory', 'file', or 'auto' | `file` |

#### Monitor-Specific Flags
| Flag | Description | Default |
|------|-------------|---------|
| `--interval` | How often to check for updates | `5m` |
| `--max-duration` | Maximum monitoring time (0 = unlimited) | `0` |
| `--stop-on-archive` | Stop when thread gets archived | `true` |
| `--stop-on-inactivity` | Stop after no new posts for this duration | `1h` |

### Configuration File

Create `~/.4archive.yaml` for persistent configuration:

```yaml
output: "/path/to/your/archive"
rate-limit: 1500
max-retries: 5
concurrency: 2
verbose: true
board: pol
content: true
media: true
posts: true
skip-existing: true

# Monitor-specific defaults
monitor:
  interval: "3m"
  max_duration: "12h"
  stop_on_archive: true
  stop_on_inactivity: "1h"
```

## Output Structure

The archiver organizes content in a structured directory layout:

```
archive-directory/
├── pol/
│   ├── 123456789/
│   │   ├── .metadata.json    # Archiving metadata and progress
│   │   ├── posts.json        # Thread posts and structure
│   │   ├── thread.json       # Original thread structure
│   │   ├── thread.db         # SQLite database with conversation analysis
│   │   ├── media/            # Media files subdirectory
│   │   │   ├── 1_filename.jpg
│   │   │   ├── 2_image.png
│   │   │   └── ...
│   │   └── ...
│   └── 987654321/
│       └── ...
└── b/
    └── ...
```

### Metadata Format

Each thread directory contains a `.metadata.json` file tracking archiving state:

```json
{
  "thread_id": "123456789",
  "board": "pol",
  "created_at": "2024-01-01T10:00:00Z",
  "last_updated": "2024-01-01T10:05:00Z",
  "posts_count": 150,
  "media_count": 45,
  "downloaded_media": {
    "https://i.4cdn.org/pol/1234567890123.jpg": {
      "filename": "1_filename.jpg",
      "url": "https://i.4cdn.org/pol/1234567890123.jpg",
      "size": 245760,
      "downloaded_at": "2024-01-01T10:01:30Z"
    }
  },
  "saved_posts": {
    "123456789": {
      "post_id": "123456789",
      "timestamp": 1704096000,
      "saved_at": "2024-01-01T10:00:30Z",
      "has_media": true
    }
  },
  "status": "completed"
}
```

## Examples

### Basic Usage

```bash
# Archive a thread with default settings
4archive archive --board pol 123456789

# Monitor an active thread for new posts
4archive monitor --board b 123456789

# Archive multiple threads with custom rate limiting
4archive archive --board b --rate-limit 2000 123456789 987654321

# Monitor multiple threads with fast updates
4archive monitor --board pol 123456789 987654321 --interval 1m --verbose

# Check what's been archived
4archive status --board pol --list
```

### Advanced Usage

```bash
# Archive with custom output directory and high concurrency
4archive archive \
  --board pol \
  --output /mnt/archive \
  --concurrency 5 \
  --rate-limit 500 \
  123456789 987654321 555666777

# Long-term monitoring with time limits
4archive monitor \
  --board b \
  --output ~/research/4chan \
  --interval 2m \
  --max-duration 24h \
  --stop-on-inactivity 2h \
  --verbose \
  123456789 987654321

# Resume interrupted archiving (skips existing files)
4archive archive --board pol 123456789

# Monitor breaking news threads with fast updates
4archive monitor --board pol 123456789 --interval 30s --stop-on-inactivity 15m

# Archive from archived.moe instead of live 4chan
4archive archive --board b --source archived.moe 123456789
```

### Workflow Examples

```bash
# Research workflow: Archive then monitor for updates
4archive archive --board pol 123456789 987654321    # Get current state
4archive monitor --board pol 123456789 987654321    # Monitor for new posts

# Breaking news monitoring
4archive monitor --board pol [thread-id] \
  --interval 1m \
  --stop-on-inactivity 30m \
  --max-duration 6h \
  --verbose

# Bulk archive with verification
4archive archive --board b 123456789 987654321 555444333
4archive status --board b 123456789 987654321 555444333 --verbose
```

## Monitoring Features

The monitoring functionality provides several key benefits:

### Real-time Updates
- **Incremental downloads**: Only downloads new posts/media since last check
- **Efficient storage**: Updates existing files instead of re-downloading everything
- **Live progress**: Shows real-time status as new content is found

### Smart Detection
- **Thread validation**: Checks threads exist before starting monitoring
- **Status detection**: Automatically detects when threads are archived or deleted
- **Error handling**: Distinguishes between temporary errors and permanent failures

### Resource Management
- **Rate limiting**: Respects 4chan's servers with configurable request rates
- **Concurrent monitoring**: Monitor multiple threads simultaneously
- **Graceful shutdown**: Responds to Ctrl+C and saves progress

### Use Cases
- **Breaking news**: Monitor threads about developing stories
- **Live events**: Capture all content from event threads
- **Research**: Collect comprehensive data over time
- **High activity**: Ensure you don't miss posts in fast-moving threads

## Development

The codebase is organized into several packages:

- `cmd/`: Cobra CLI commands and configuration
- `internal/archiver/`: Core archiving and monitoring logic with rate limiting
- `internal/metadata/`: Metadata management and persistence
- `internal/analysis/`: Conversation analysis and post parsing
- `internal/database/`: SQLite database management with sqlc

### Building from Source

```bash
go mod tidy
go build -o 4archive
```

### Running Tests

```bash
# Run all tests
go test ./...

# Run specific test suites (respect the rule: never test all at once)
go test -v ./internal/archiver -run TestFindNewPosts
go test -v ./internal/analysis -run TestConversationAnalysis
```

## Architecture

The archiver uses several key components:

1. **Rate Limiter**: Controls request frequency to avoid overwhelming servers
2. **Metadata Manager**: Tracks archiving state and prevents duplicate downloads
3. **Concurrent Downloader**: Downloads multiple threads/files simultaneously with controlled concurrency
4. **Retry Logic**: Handles network failures gracefully with exponential backoff using `github.com/sethvargo/go-retry`
5. **Thread Monitor**: Continuously monitors active threads with intelligent error detection
6. **Conversation Analyzer**: Parses post relationships and reply structures
7. **Multi-source Support**: Works with both live 4chan and archived.moe

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable (follow the testing rules in the codebase)
5. Submit a pull request

## License

MIT License - see LICENSE file for details. 