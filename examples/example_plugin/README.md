# Example Databot Plugin

This example demonstrates how to create a custom tool plugin for databot.

## Structure

```
example_plugin/
├── pyproject.toml    # Package config with entry points
├── __init__.py       # Package init
└── tools.py          # Custom tool implementation
```

## Installation

```bash
# From the examples/example_plugin directory:
pip install -e .
```

## How It Works

1. Create a tool class inheriting from `BaseTool`
2. Implement `name`, `description`, `parameters()`, and `execute()`
3. Register via entry points in `pyproject.toml`
4. Install the package - databot will auto-discover it

## Testing

After installation, start databot and your tool will be available:

```bash
databot agent -m "Use the hello_world tool to greet Alice"
```
