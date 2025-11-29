# Contributing

Thank you for your interest in contributing to the Apache Airflow MCP Server!

## Development Setup

```bash
# Clone the repository
git clone https://github.com/madamak/apache-airflow-mcp-server.git
cd apache-airflow-mcp-server

# Install dependencies
uv sync

# Run tests
uv run pytest

# Run linter
uv run ruff check .

# Format code
uv run ruff format .
```

## Making Changes

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run tests and linting
5. Commit your changes (`git commit -am 'Add my feature'`)
6. Push to the branch (`git push origin feature/my-feature`)
7. Open a Pull Request

## Code Style

- We use [Ruff](https://docs.astral.sh/ruff/) for linting and formatting
- Line length limit: 100 characters
- Follow existing code patterns

## Testing

- Add tests for new functionality
- Ensure all tests pass before submitting PR
- Tests should not require real Airflow instances (use mocks)

## Pull Request Guidelines

- Keep PRs focused on a single change
- Update documentation if needed
- Add tests for new features
- Ensure CI passes

## Reporting Issues

- Use GitHub Issues for bug reports and feature requests
- Include steps to reproduce for bugs
- Check existing issues before creating new ones

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
