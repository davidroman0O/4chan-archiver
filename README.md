# 4chan Archiver

A comprehensive CLI tool for archiving 4chan threads with controlled downloading, metadata tracking, and support for multiple boards.

## Features

- **Controlled Downloads**: Rate limiting and retry mechanisms to be respectful to servers
- **Metadata Tracking**: JSON metadata files track archiving progress and prevent duplicate downloads
- **Multi-thread Support**: Archive multiple threads concurrently
- **Board Support**: Works with any 4chan board (pol, b, etc.)
- **Flexible Content Types**: Choose what to archive (posts, media, or both)
- **Resume Capability**: Skip already downloaded content on subsequent runs
- **Status Monitoring**: Check archiving progress and view archived thread information

## Installation

```bash
# Clone the repository
git clone https://github.com/davidroman0O/4chan-archiver
cd 4chan-archiver

# Build the application
go build -o 4archive

# Or install directly
go install
```

## Usage

### Archive Threads

Archive one or multiple threads:

```bash
# Archive a single thread from /pol/
./4archive archive --board pol 123456789

# Archive multiple threads
./4archive archive --board b 123456789 987654321 555666777

# Archive with specific content types
./4archive archive --board pol --media --posts 123456789

# Archive to a specific directory
./4archive archive --board pol --output /path/to/archive 123456789

# Control download behavior
./4archive archive --board pol --rate-limit 2000 --concurrency 2 123456789
```

### Check Status

View archiving status and metadata:

```bash
# Check status of specific threads
./4archive status --board pol 123456789 987654321

# List all archived threads for a board
./4archive status --board pol --list

# Check status with verbose output
./4archive status --board pol --verbose 123456789
```

## Configuration

### Command Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--board, -b` | Board name (pol, b, etc.) | `pol` |
| `--output, -o` | Output directory | `~/Documents/4archive` |
| `--rate-limit` | Rate limit between requests (ms) | `1000` |
| `--max-retries` | Maximum retries for failed requests | `3` |
| `--concurrency, -c` | Maximum concurrent downloads | `3` |
| `--user-agent` | Custom user agent | Random |
| `--verbose, -v` | Enable verbose logging | `false` |
| `--content` | Include thread content/posts | `true` |
| `--media` | Include media files | `true` |
| `--posts` | Include post text and metadata | `true` |
| `--skip-existing` | Skip files that already exist | `true` |

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
```

## Output Structure

The archiver organizes content in a structured directory layout:

```
archive-directory/
├── pol/
│   ├── 123456789/
│   │   ├── .metadata.json    # Archiving metadata and progress
│   │   ├── posts.json        # Thread posts and structure
│   │   ├── 1234567890123.jpg # Media files
│   │   ├── 1234567890124.png
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
      "filename": "1234567890123.jpg",
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
./4archive archive --board pol 123456789

# Archive multiple threads with custom rate limiting
./4archive archive --board b --rate-limit 2000 123456789 987654321

# Archive only media files (no posts)
./4archive archive --board pol --no-posts 123456789

# Check what's been archived
./4archive status --board pol --list
```

### Advanced Usage

```bash
# Archive with custom output directory and high concurrency
./4archive archive \
  --board pol \
  --output /mnt/archive \
  --concurrency 5 \
  --rate-limit 500 \
  123456789 987654321 555666777

# Resume interrupted archiving (skips existing files)
./4archive archive --board pol 123456789

# Verbose monitoring of large archive operation
./4archive archive --verbose --board b \
  $(seq 123456789 123456799)
```

## Development

The codebase is organized into several packages:

- `cmd/`: Cobra CLI commands and configuration
- `internal/archiver/`: Core archiving logic with rate limiting
- `internal/metadata/`: Metadata management and persistence

### Building from Source

```bash
go mod tidy
go build -o 4archive
```

### Running Tests

```bash
go test ./...
```

## Architecture

The archiver uses several key components:

1. **Rate Limiter**: Controls request frequency to avoid overwhelming servers
2. **Metadata Manager**: Tracks archiving state and prevents duplicate downloads
3. **Concurrent Downloader**: Downloads multiple threads/files simultaneously with controlled concurrency
4. **Retry Logic**: Handles network failures gracefully with exponential backoff

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - see LICENSE file for details. 